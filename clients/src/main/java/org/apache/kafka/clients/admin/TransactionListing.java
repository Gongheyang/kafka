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
package org.apache.kafka.clients.admin;

import java.util.Objects;

public class TransactionListing {
    private final String transactionalId;
    private final long producerId;
    private final TransactionState transactionState;

    public TransactionListing(
        String transactionalId,
        long producerId,
        TransactionState transactionState
    ) {
        this.transactionalId = transactionalId;
        this.producerId = producerId;
        this.transactionState = transactionState;
    }

    public String transactionalId() {
        return transactionalId;
    }

    public long producerId() {
        return producerId;
    }

    public TransactionState transactionState() {
        return transactionState;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TransactionListing that = (TransactionListing) o;
        return producerId == that.producerId &&
            Objects.equals(transactionalId, that.transactionalId) &&
            transactionState == that.transactionState;
    }

    @Override
    public int hashCode() {
        return Objects.hash(transactionalId, producerId, transactionState);
    }
}
