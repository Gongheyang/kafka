/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka

import kafka.tools.StorageTool

object KafkaDockerWrapper {

  private def prepareConfigs(defaultConfigsDir: String, realConfigsDir: String): Unit = {

  }

  private def formatStorageCmd(configsDir: String): Array[String] = {
//    format --cluster-id=$CLUSTER_ID -c /opt/kafka/config/server.properties
    Array.empty[String]
  }

  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      throw new RuntimeException(s"Error: No operation input provided. " +
        s"Please provide a valid operation: 'setup'.")
    }
    val operation = args.head
    val arguments = args.tail

    operation match {
      case "setup" =>
        val defaultConfigsDir = arguments(0)
        val realConfigsDir = arguments(1)
        prepareConfigs(defaultConfigsDir, realConfigsDir)

        val formatCmd = formatStorageCmd(realConfigsDir)
        StorageTool.main(formatCmd)
      case _ =>
        throw new RuntimeException(s"Unknown operation $operation. " +
          s"Please provide a valid operation: 'setup'.")
    }
  }
}
