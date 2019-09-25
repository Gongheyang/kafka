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

package org.apache.kafka.streams.kstream;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;

/**
 * Class used to configure the name of the join processor, the repartition topic name,
 * state stores or state store names in  Stream-Stream join.
 * @param <K>   the key type
 * @param <V1>  this value type
 * @param <V2>  other value type
 */
public class StreamJoined<K, V1, V2>
    implements NamedOperation<StreamJoined<K, V1, V2>> {

    protected final Serde<K> keySerde;
    protected final Serde<V1> valueSerde;
    protected final Serde<V2> otherValueSerde;
    protected final WindowBytesStoreSupplier thisStoreSupplier;
    protected final WindowBytesStoreSupplier otherStoreSupplier;
    protected final String name;
    protected final String storeName;

    protected StreamJoined(final StreamJoined<K, V1, V2> streamJoined) {
        this(streamJoined.keySerde,
            streamJoined.valueSerde,
            streamJoined.otherValueSerde,
            streamJoined.thisStoreSupplier,
            streamJoined.otherStoreSupplier,
            streamJoined.name,
            streamJoined.storeName);
    }

    private StreamJoined(final Serde<K> keySerde,
                         final Serde<V1> valueSerde,
                         final Serde<V2> otherValueSerde,
                         final WindowBytesStoreSupplier thisStoreSupplier,
                         final WindowBytesStoreSupplier otherStoreSupplier,
                         final String name,
                         final String storeName) {
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.otherValueSerde = otherValueSerde;
        this.thisStoreSupplier = thisStoreSupplier;
        this.otherStoreSupplier = otherStoreSupplier;
        this.name = name;
        this.storeName = storeName;
    }

    /**
     * Creates a StreamJoined instance with the provided store suppliers. The store suppliers must implement
     * the {@link WindowBytesStoreSupplier} interface.  The store suppliers must provide unique names or a
     * {@link org.apache.kafka.streams.errors.StreamsException} is thrown.
     *
     * @param storeSupplier       this store supplier
     * @param otherStoreSupplier  other store supplier
     * @param <K>                 the key type
     * @param <V1>                this value type
     * @param <V2>                other value type
     * @return                    {@link StreamJoined} instance
     */
    public static <K, V1, V2> StreamJoined<K, V1, V2> with(final WindowBytesStoreSupplier storeSupplier,
                                                           final WindowBytesStoreSupplier otherStoreSupplier) {
        return new StreamJoined<>(
            null,
            null,
            null,
            storeSupplier,
            otherStoreSupplier,
            null,
            null
        );
    }

    /**
     * Creates a {@link StreamJoined} instance using the provided name for the state stores and hence the changelog
     * topics for the join stores.  The name for the stores will be ${applicationId}-&lt;storeName&gt;-this-join and ${applicationId}-&lt;storeName&gt;-other-join
     * or ${applicationId}-&lt;storeName&gt;-outer-this-join and ${applicationId}-&lt;storeName&gt;-outer-other-join depending if the join is an inner-join
     * or an outer join. The changelog topics will have the -changelog suffix.
     *
     * @param storeName  The name to use for the store
     * @param <K>        The key type
     * @param <V1>       This value type
     * @param <V2>       Other value type
     * @return            {@link StreamJoined} instance
     */
    public static <K, V1, V2> StreamJoined<K, V1, V2> as(final String storeName) {
        return new StreamJoined<>(
            null,
            null,
            null,
            null,
            null,
            null,
            storeName
        );
    }


    /**
     * Creates a {@link StreamJoined} instance with the provided serdes to configure the stores
     * for the join.
     * @param keySerde          The key serde
     * @param valueSerde        This value serde
     * @param otherValueSerde   Other value serde
     * @param <K>               The key type
     * @param <V1>              This value type
     * @param <V2>              Other value type
     * @return                  {@link StreamJoined} instance
     */
    public static <K, V1, V2> StreamJoined<K, V1, V2> with(final Serde<K> keySerde,
                                                           final Serde<V1> valueSerde,
                                                           final Serde<V2> otherValueSerde
    ) {
        return new StreamJoined<>(
            keySerde,
            valueSerde,
            otherValueSerde,
            null,
            null,
            null,
            null
        );
    }

    /**
     * Set the name to use for the join processor and the repartition topic(s) if required.
     * @param name  the name to use
     * @return      a new {@link StreamJoined} instance
     */
    @Override
    public StreamJoined<K, V1, V2> withName(final String name) {
        return new StreamJoined<>(
            keySerde,
            valueSerde,
            otherValueSerde,
            thisStoreSupplier,
            otherStoreSupplier,
            name,
            storeName
        );
    }

    /**
     * Sets the base store name to use for both sides of the join. The name for the state stores and hence the changelog
     * topics for the join stores.  The name for the stores will be ${applicationId}-&lt;storeName&gt;-this-join and ${applicationId}-&lt;storeName&gt;-other-join
     * or ${applicationId}-&lt;storeName&gt;-outer-this-join and ${applicationId}-&lt;storeName&gt;-outer-other-join depending if the join is an inner-join
     * or an outer join. The changelog topics will have the -changelog suffix.
     *
     * @param storeName the storeName to use
     * @return
     */
    public StreamJoined<K, V1, V2> withStoreName(final String storeName) {
        return new StreamJoined<>(
            keySerde,
            valueSerde,
            otherValueSerde,
            thisStoreSupplier,
            otherStoreSupplier,
            name,
            storeName
        );
    }

    /**
     * Configure with the provided {@link Serde<K>} for the key
     * @param keySerde  the serde to use for the key
     * @return          a new {@link StreamJoined} configured with the keySerde
     */
    public StreamJoined<K, V1, V2> withKeySerde(final Serde<K> keySerde) {
        return new StreamJoined<>(
            keySerde,
            valueSerde,
            otherValueSerde,
            thisStoreSupplier,
            otherStoreSupplier,
            name,
            storeName
        );
    }

    /**
     * Configure with the provided {@link Serde<V1>} for this value
     * @param valueSerde  the serde to use for this value (calling or left side of the join)
     * @return            a new {@link StreamJoined} configured with the valueSerde
     */
    public StreamJoined<K, V1, V2> withValueSerde(final Serde<V1> valueSerde) {
        return new StreamJoined<>(
            keySerde,
            valueSerde,
            otherValueSerde,
            thisStoreSupplier,
            otherStoreSupplier,
            name,
            storeName
        );
    }

    /**
     * Configure with the provided {@link Serde<V2>} for the other value
     * @param otherValueSerde  the serde to use for the other value (other or right side of the join)
     * @return                 a new {@link StreamJoined} configured with the otherValueSerde
     */
    public StreamJoined<K, V1, V2> withOtherValueSerde(final Serde<V2> otherValueSerde) {
        return new StreamJoined<>(
            keySerde,
            valueSerde,
            otherValueSerde,
            thisStoreSupplier,
            otherStoreSupplier,
            name,
            storeName
        );
    }

    /**
     * Configure with the provided {@link WindowBytesStoreSupplier} for this store supplier.  Please note
     * this method only provides the store supplier for the left side of the join.  If you wish to also provide a
     * store supplier for the "other" side you must use the {@link StreamJoined#withOtherStoreSupplier(WindowBytesStoreSupplier)}
     * method
     * @param thisStoreSupplier  the store supplier to use for this store supplier (calling or left side of the join)
     * @return            a new {@link StreamJoined} configured with thisStoreSupplier
     */
    public StreamJoined<K, V1, V2> withThisStoreSupplier(final WindowBytesStoreSupplier thisStoreSupplier) {
        return new StreamJoined<>(
            keySerde,
            valueSerde,
            otherValueSerde,
            thisStoreSupplier,
            otherStoreSupplier,
            name,
            storeName
        );
    }

    /**
     * Configure with the provided {@link WindowBytesStoreSupplier} for the other store supplier.  Please note
     * this method only provides the store supplier for the right side of the join.  If you wish to also provide a
     * store supplier for the "left" side you must use the {@link StreamJoined#withThisStoreSupplier(WindowBytesStoreSupplier)}
     * method
     * @param otherStoreSupplier  the store supplier to use for the other store supplier (other or right side of the join)
     * @return            a new {@link StreamJoined} configured with otherStoreSupplier
     */
    public StreamJoined<K, V1, V2> withOtherStoreSupplier(final WindowBytesStoreSupplier otherStoreSupplier) {
        return new StreamJoined<>(
            keySerde,
            valueSerde,
            otherValueSerde,
            thisStoreSupplier,
            otherStoreSupplier,
            name,
            storeName
        );
    }

    @Override
    public String toString() {
        return "StreamJoin{" +
            "keySerde=" + keySerde +
            ", valueSerde=" + valueSerde +
            ", otherValueSerde=" + otherValueSerde +
            ", thisStoreSupplier=" + thisStoreSupplier +
            ", otherStoreSupplier=" + otherStoreSupplier +
            ", name='" + name + '\'' +
            ", storeName='" + storeName + '\'' +
            '}';
    }
}
