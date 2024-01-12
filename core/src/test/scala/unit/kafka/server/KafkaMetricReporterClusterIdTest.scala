/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.server

import kafka.integration.KafkaServerTestHarness

import java.util.concurrent.atomic.AtomicReference
import kafka.metrics.KafkaMetricsReporter
import kafka.utils.{TestUtils, VerifiableProperties}
import org.apache.kafka.common.{ClusterResource, ClusterResourceListener}
import org.apache.kafka.test.MockMetricsReporter
import org.junit.jupiter.api.Assertions._
import org.apache.kafka.test.TestUtils.isValidClusterId
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import kafka.utils.TestInfoUtils

import java.util.Properties
import scala.collection.Seq

object KafkaMetricReporterClusterIdTest {
  val setupError = new AtomicReference[String]("")

  class MockKafkaMetricsReporter extends KafkaMetricsReporter with ClusterResourceListener {

    override def onUpdate(clusterMetadata: ClusterResource): Unit = {
      MockKafkaMetricsReporter.CLUSTER_META.set(clusterMetadata)
    }

    override def init(props: VerifiableProperties): Unit = {
    }
  }

  object MockKafkaMetricsReporter {
    val CLUSTER_META = new AtomicReference[ClusterResource]
  }

  object MockBrokerMetricsReporter {
    val CLUSTER_META: AtomicReference[ClusterResource] = new AtomicReference[ClusterResource]
  }

  class MockBrokerMetricsReporter extends MockMetricsReporter with ClusterResourceListener {

    override def onUpdate(clusterMetadata: ClusterResource): Unit = {
      MockBrokerMetricsReporter.CLUSTER_META.set(clusterMetadata)
    }

    override def configure(configs: java.util.Map[String, _]): Unit = {
      // Check that the configuration passed to the MetricsReporter includes the broker id as an Integer.
      // This is a regression test for KAFKA-4756.
      //
      // Because this code is run during the test setUp phase, if we throw an exception here,
      // it just results in the test itself being declared "not found" rather than failing.
      // So we track an error message which we will check later in the test body.
      val brokerId = configs.get(KafkaConfig.BrokerIdProp)
      if (brokerId == null)
        setupError.compareAndSet("", "No value was set for the broker id.")
      else if (!brokerId.isInstanceOf[String])
        setupError.compareAndSet("", "The value set for the broker id was not a string.")
      try
        Integer.parseInt(brokerId.asInstanceOf[String])
      catch {
        case e: Exception => setupError.compareAndSet("", "Error parsing broker id " + e.toString)
      }
    }
  }
}

class KafkaMetricReporterClusterIdTest extends KafkaServerTestHarness {

  def generateConfigs: Seq[KafkaConfig] = {
    val overridingProps = new Properties
    overridingProps.setProperty(KafkaConfig.KafkaMetricsReporterClassesProp, "kafka.server.KafkaMetricReporterClusterIdTest$MockKafkaMetricsReporter")
    overridingProps.setProperty(KafkaConfig.MetricReporterClassesProp, "kafka.server.KafkaMetricReporterClusterIdTest$MockBrokerMetricsReporter")
    overridingProps.setProperty(KafkaConfig.BrokerIdGenerationEnableProp, "true")
    TestUtils.createBrokerConfigs(1, zkConnectOrNull).
      map(KafkaConfig.fromProps(_, overridingProps))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk","kraft"))
  def testClusterIdPresent(quorum: String): Unit = {
    assertEquals("", KafkaMetricReporterClusterIdTest.setupError.get())

    assertNotNull(KafkaMetricReporterClusterIdTest.MockBrokerMetricsReporter.CLUSTER_META)
    isValidClusterId(KafkaMetricReporterClusterIdTest.MockBrokerMetricsReporter.CLUSTER_META.get().clusterId())

    if(!isKRaftTest()) {
      assertNotNull(KafkaMetricReporterClusterIdTest.MockKafkaMetricsReporter.CLUSTER_META)
      isValidClusterId(KafkaMetricReporterClusterIdTest.MockKafkaMetricsReporter.CLUSTER_META.get().clusterId())

      assertEquals(KafkaMetricReporterClusterIdTest.MockKafkaMetricsReporter.CLUSTER_META.get().clusterId(),
        KafkaMetricReporterClusterIdTest.MockBrokerMetricsReporter.CLUSTER_META.get().clusterId())
    }

    TestUtils.assertNoNonDaemonThreads(this.getClass.getName)
  }

}