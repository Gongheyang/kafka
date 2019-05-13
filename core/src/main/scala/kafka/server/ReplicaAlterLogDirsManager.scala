/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  *    http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package kafka.server

import kafka.cluster.BrokerEndPoint
import org.apache.kafka.common.TopicPartition
import scala.collection.Set

class ReplicaAlterLogDirsManager(brokerConfig: KafkaConfig,
                                 replicaManager: ReplicaManager,
                                 quotaManager: ReplicationQuotaManager,
                                 brokerTopicStats: BrokerTopicStats)
  extends AbstractFetcherManager[ReplicaAlterLogDirsThread](
    name = s"ReplicaAlterLogDirsManager on broker ${brokerConfig.brokerId}",
    clientId = "ReplicaAlterLogDirs",
    numFetchers = brokerConfig.getNumReplicaAlterLogDirsThreads) {

  override def createFetcherThread(fetcherId: Int, sourceBroker: BrokerEndPoint, markPartitionsFailed: (Set[TopicPartition]) => Unit): ReplicaAlterLogDirsThread = {
    val threadName = s"ReplicaAlterLogDirsThread-$fetcherId"
    new ReplicaAlterLogDirsThread(threadName, sourceBroker, brokerConfig, markPartitionsFailed, replicaManager,
      quotaManager, brokerTopicStats)
  }

  def shutdown() {
    info("shutting down")
    closeAllFetchers()
    info("shutdown completed")
  }
}
