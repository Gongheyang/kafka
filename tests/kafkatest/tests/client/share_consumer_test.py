# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from ducktape.mark import matrix
from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until
from kafkatest.tests.verifiable_share_consumer_test import VerifiableShareConsumerTest

from kafkatest.services.kafka import TopicPartition, quorum

class ShareTest(VerifiableShareConsumerTest):
    TOPIC1 = {"name": "test_topic1", "partitions": 1,"replication_factor": 1}
    TOPIC2 = {"name": "test_topic2", "partitions": 1,"replication_factor": 1}
    TOPIC3 = {"name": "test_topic3", "partitions": 1,"replication_factor": 1}

    def __init__(self, test_context):
        super(ShareTest, self).__init__(test_context, num_consumers=1, num_producers=1,
                                                     num_zk=0, num_brokers=1, topics={
            self.TOPIC1["name"] : { 'partitions': self.TOPIC1["partitions"], 'replication-factor': self.TOPIC1["replication_factor"] },
            self.TOPIC2["name"] : { 'partitions': self.TOPIC2["partitions"], 'replication-factor': self.TOPIC2["replication_factor"] }
        })

    def setup_share_group(self, topic, **kwargs):
        consumer = super(ShareTest, self).setup_share_group(topic, **kwargs)
        self.mark_for_collect(consumer, 'verifiable_share_consumer_stdout')
        return consumer
    
    def get_topic_partitions(self, topic):
        return [TopicPartition(topic["name"], i) for i in range(topic["partitions"])]
    
    def wait_until_topic_replicas_settled(self, topic, timeout_sec=60):
        for partition in range(0, topic["partitions"]):
            wait_until(lambda: len(self.kafka.isr_idx_list(topic["name"], partition)) == topic["replication_factor"], 
                    timeout_sec=timeout_sec, backoff_sec=1, err_msg="Replicas did not rejoin the ISR in a reasonable amount of time")
    
    def rolling_bounce_brokers(self, topic, num_bounces=5, clean_shutdown=True):
        for _ in range(num_bounces):
            for i in range(len(self.kafka.nodes)):
                node = self.kafka.nodes[i]
                self.kafka.restart_node(node, clean_shutdown=clean_shutdown)
                self.wait_until_topic_replicas_settled(topic, timeout_sec=60)

    @cluster(num_nodes=10)
    @matrix(
        metadata_quorum=[quorum.isolated_kraft]
    )
    def test_share_single_topic_partition(self, metadata_quorum=quorum.isolated_kraft):

        total_messages = 100000
        producer = self.setup_producer(self.TOPIC1["name"], max_messages=total_messages)

        consumer = self.setup_share_group(self.TOPIC1["name"], offset_reset_strategy="earliest")

        producer.start()

        consumer.start()
        self.await_all_members(consumer, timeout_sec=60)

        self.await_acknowledged_messages(consumer, min_messages=total_messages, timeout_sec=600)

        assert consumer.total_consumed() >= producer.num_acked
        assert consumer.total_acknowledged() == producer.num_acked

        for event_handler in consumer.event_handlers.values():
            assert event_handler.total_consumed > 0
            assert event_handler.total_acknowledged > 0

        consumer.stop_all()

    @cluster(num_nodes=10)
    @matrix(
        metadata_quorum=[quorum.isolated_kraft]
    )
    def test_share_multiple_partitions(self, metadata_quorum=quorum.isolated_kraft):

        total_messages = 1000000
        producer = self.setup_producer(self.TOPIC2["name"], max_messages=total_messages, throughput=5000)

        consumer = self.setup_share_group(self.TOPIC2["name"], offset_reset_strategy="earliest")

        producer.start()

        consumer.start()
        self.await_all_members(consumer, timeout_sec=60)

        self.await_acknowledged_messages(consumer, min_messages=total_messages, timeout_sec=600)

        assert consumer.total_consumed() >= producer.num_acked
        assert consumer.total_acknowledged() == producer.num_acked

        for event_handler in consumer.event_handlers.values():
            assert event_handler.total_consumed > 0
            assert event_handler.total_acknowledged > 0
            for topic_partition in self.get_topic_partitions(self.TOPIC2):
                assert topic_partition in event_handler.consumed_per_partition
                assert event_handler.consumed_per_partition[topic_partition] > 0
                assert topic_partition in event_handler.acknowledged_per_partition
                assert event_handler.acknowledged_per_partition[topic_partition] > 0

        consumer.stop_all()

    @cluster(num_nodes=10)
    @matrix(
        metadata_quorum=[quorum.isolated_kraft]
    )
    def test_broker_rolling_bounce(self, metadata_quorum=quorum.isolated_kraft):

        producer = self.setup_producer(self.TOPIC2["name"])
        consumer = self.setup_share_group(self.TOPIC2["name"], offset_reset_strategy="earliest")

        producer.start()
        self.await_produced_messages(producer)

        consumer.start()
        self.await_all_members(consumer)

        self.await_consumed_messages(consumer, timeout_sec=60)
        self.rolling_bounce_brokers(self.TOPIC2, num_bounces=1, clean_shutdown=True)

        producer.stop()

        self.await_unique_consumed_messages(consumer, min_messages=producer.num_acked, timeout_sec=240)
        # self.await_unique_acknowledged_messages(consumer, min_messages=producer.num_acked, timeout_sec=600)

        assert consumer.total_unique_consumed() >= producer.num_acked
        # assert consumer.total_unique_acknowledged() >= producer.num_acked

        consumer.stop_all()