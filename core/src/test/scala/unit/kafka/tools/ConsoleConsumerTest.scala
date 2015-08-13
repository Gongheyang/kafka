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

package unit.kafka.tools

import kafka.consumer.{BaseConsumer, BaseConsumerRecord}
import kafka.tools.{ConsoleConsumer, MessageFormatter}
import org.easymock.EasyMock
import org.junit.Assert._
import org.junit.Test
import org.scalatest.junit.JUnit3Suite

class ConsoleConsumerTest extends JUnit3Suite {

  override def setUp() {
    super.setUp()
  }

  override def tearDown() {
    super.tearDown()
  }

  @Test
  def testShouldLimitReadsToMaxMessageLimit() {
    //Mocks
    val consumer = EasyMock.createNiceMock(classOf[BaseConsumer])
    val formatter = EasyMock.createNiceMock(classOf[MessageFormatter])

    //Stubs
    val record = new BaseConsumerRecord("foo", 1, 1, Array[Byte](), Array[Byte]())

    //Expectations
    val messageLimit: Int = 10
    EasyMock.expect(formatter.writeTo(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject())).times(messageLimit)
    EasyMock.expect(consumer.receive()).andReturn(record).times(messageLimit)

    EasyMock.replay(consumer)
    EasyMock.replay(formatter)

    //Test
    ConsoleConsumer.process(messageLimit, formatter, consumer)
  }

  @Test
  def testShouldParseValidOldConsumerValidConfig() {
    //Given
    val args: Array[String] = Array(
      "--zookeeper", "localhost:2181",
      "--topic", "test",
      "--from-beginning")

    //When
    val config = new ConsoleConsumer.ConsumerConfig(args)

    //Then
    assertFalse(config.useNewConsumer)
    assertEquals("localhost:2181", config.zkConnectionStr)
    assertEquals("test", config.topicArg)
    assertEquals(true, config.fromBeginning)
  }

  @Test
  def testShouldParseValidNewConsumerValidConfig() {
    //Given
    val args: Array[String] = Array(
      "--bootstrap-server", "localhost:9092",
      "--topic", "test",
      "--from-beginning",
      "--new-consumer") //new

    //When
    val config = new ConsoleConsumer.ConsumerConfig(args)

    //Then
    assertTrue(config.useNewConsumer)
    assertEquals("localhost:9092", config.bootstrapServer)
    assertEquals("test", config.topicArg)
    assertEquals(true, config.fromBeginning)
  }

}
