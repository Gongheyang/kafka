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
package org.apache.kafka.clients.consumer;

/**
 * Enum to specify the group membership operation upon leaving group.
 *
 * <ul>
 *   <li><b>{@code LEAVE_GROUP}</b>:  means the consumer will leave the group.</li>
 *   <li><b>{@code REMAIN_IN_GROUP}</b>: means the consumer will remain in the group.</li>
 *   <li><b>{@code DEFAULT}</b>: Applies the default behavior:
 *     <ul>
 *       <li>For <b>static members</b>: The consumer will remain in the group.</li>
 *       <li>For <b>dynamic members</b>: The consumer will leave the group.</li>
 *     </ul>
 *   </li>
 * </ul>
 */
public enum GroupMembershipOperation {
    LEAVE_GROUP,
    REMAIN_IN_GROUP,
    DEFAULT
}
