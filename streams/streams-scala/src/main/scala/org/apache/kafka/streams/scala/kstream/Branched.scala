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
package org.apache.kafka.streams.scala.kstream

import org.apache.kafka.streams.kstream.{Branched => BranchedJ, KStream => KStreamJ}

import scala.jdk.FunctionConverters._

object Branched {

  /**
   * Create an instance of `Branched` with provided branch name suffix.
   *
   * @param name the branch name suffix to be used. If `null`, a default branch name suffix will be generated
   *             (see [[BranchedKStream]] description for details)
   * @tparam K key type
   * @tparam V value type
   * @return a new instance of `Branched`
   */
  def as[K, V](name: String): BranchedJ[K, V] =
    BranchedJ.as[K, V](name)

  /**
   * Create an instance of `Branched` with provided chain function and branch name suffix.
   *
   * @param chain A function that will be applied to the branch. If `null`, the identity
   *              function will be used. If the provided function returns
   *              `null`, its result is ignored, otherwise it is added to the Map returned
   *              by [[BranchedKStream.defaultBranch]] or [[BranchedKStream.noDefaultBranch]] (see
   *              [[BranchedKStream]] description for details).
   * @param name  the branch name suffix to be used. If `null`, a default branch name suffix will be generated
   *              (see [[BranchedKStream]] description for details)
   * @tparam K key type
   * @tparam V value type
   * @return a new instance of `Branched`
   * @see `org.apache.kafka.streams.kstream.Branched#withFunction(java.util.function.Function, java.lang.String)`
   */
  def withFunction[K, V](chain: KStream[K, V] => KStream[K, V], name: String = null): BranchedJ[K, V] =
    BranchedJ.withFunction({ s: KStreamJ[K, V] =>
      chain.apply(new KStream[K, V](s)).inner
    }.asJava, name)

  /**
   * Create an instance of `Branched` with provided chain function and branch name suffix.
   *
   * @param chain A consumer to which the branch will be sent. If a non-null consumer is provided here,
   *              the respective branch will not be added to the resulting Map returned
   *              by [[BranchedKStream.defaultBranch]] or [[BranchedKStream.noDefaultBranch]] (see
   *              [[BranchedKStream]] description for details). If `null`, a no-op consumer will be used
   *              and the branch will be added to the resulting Map.
   * @param name  the branch name suffix to be used. If `null`, a default branch name suffix will be generated
   *              (see [[BranchedKStream]] description for details)
   * @tparam K key type
   * @tparam V value type
   * @return a new instance of `Branched`
   * @see `org.apache.kafka.streams.kstream.Branched#withConsumer(java.util.function.Consumer, java.lang.String)`
   */
  def withConsumer[K, V](chain: KStream[K, V] => Unit, name: String = null): BranchedJ[K, V] =
    BranchedJ.withConsumer({ s: KStreamJ[K, V] =>
      chain.apply(new KStream[K, V](s))
    }.asJava, name)
}
