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
package org.apache.kafka.clients.consumer.internals.events;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkThread;
import org.apache.kafka.clients.consumer.internals.ConsumerRebalanceListenerInvoker;
import org.apache.kafka.clients.consumer.internals.ConsumerRebalanceListenerMethodName;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.util.Optional;
import java.util.SortedSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An {@link EventProcessor} that is created and executes in the application thread for the purpose of processing
 * {@link BackgroundEvent background events} generated by the {@link ConsumerNetworkThread network thread}.
 * Those events are generally of two types:
 *
 * <ul>
 *     <li>Errors that occur in the network thread that need to be propagated to the application thread</li>
 *     <li>{@link ConsumerRebalanceListener} callbacks that are to be executed on the application thread</li>
 * </ul>
 */
public class BackgroundEventProcessor extends EventProcessor<BackgroundEvent> {

    private final Logger log;
    private final ApplicationEventHandler applicationEventHandler;
    private final ConsumerRebalanceListenerInvoker rebalanceListenerInvoker;

    public BackgroundEventProcessor(final LogContext logContext,
                                    final BlockingQueue<BackgroundEvent> backgroundEventQueue,
                                    final ApplicationEventHandler applicationEventHandler,
                                    final ConsumerRebalanceListenerInvoker rebalanceListenerInvoker) {
        super(logContext, backgroundEventQueue);
        this.log = logContext.logger(BackgroundEventProcessor.class);
        this.applicationEventHandler = applicationEventHandler;
        this.rebalanceListenerInvoker = rebalanceListenerInvoker;
    }

    /**
     * Process the events—if any—that were produced by the {@link ConsumerNetworkThread network thread}.
     * It is possible that {@link ErrorBackgroundEvent an error} could occur when processing the events.
     * In such cases, the processor will take a reference to the first error, continue to process the
     * remaining events, and then throw the first error that occurred.
     */
    public void process() {
        AtomicReference<KafkaException> firstError = new AtomicReference<>();

        ProcessHandler<BackgroundEvent> processHandler = (event, error) -> {
            if (error.isPresent()) {
                KafkaException e = error.get();

                if (!firstError.compareAndSet(null, e)) {
                    log.warn("An error occurred when processing the event: {}", e.getMessage(), e);
                }
            }
        };

        process(processHandler);

        if (firstError.get() != null)
            throw firstError.get();
    }

    @Override
    public void process(final BackgroundEvent event) {
        switch (event.type()) {
            case ERROR:
                process((ErrorBackgroundEvent) event);
                break;

            case CONSUMER_REBALANCE_LISTENER_CALLBACK_NEEDED:
                process((ConsumerRebalanceListenerCallbackNeededEvent) event);
                break;

            default:
                throw new IllegalArgumentException("Background event type " + event.type() + " was not expected");
        }
    }

    private void process(final ErrorBackgroundEvent event) {
        throw event.error();
    }

    private void process(final ConsumerRebalanceListenerCallbackNeededEvent event) {
        SortedSet<TopicPartition> partitions = event.partitions();
        ConsumerRebalanceListenerMethodName methodName = event.methodName();
        final Exception e;

        switch (methodName) {
            case onPartitionsRevoked:
                e = rebalanceListenerInvoker.invokePartitionsRevoked(partitions);
                break;

            case onPartitionsAssigned:
                e = rebalanceListenerInvoker.invokePartitionsAssigned(partitions);
                break;

            case onPartitionsLost:
                e = rebalanceListenerInvoker.invokePartitionsLost(partitions);
                break;

            default:
                throw new IllegalArgumentException("Could not determine the " + ConsumerRebalanceListener.class.getSimpleName() + " to invoke from the callback method " + methodName);
        }

        final Optional<KafkaException> error;

        if (e != null) {
            if (e instanceof KafkaException)
                error = Optional.of((KafkaException) e);
            else
                error = Optional.of(new KafkaException("User rebalance callback throws an error", e));
        } else {
            error = Optional.empty();
        }

        ApplicationEvent invokedEvent = new ConsumerRebalanceListenerCallbackCompletedEvent(methodName, partitions, error);
        applicationEventHandler.add(invokedEvent);
    }
}
