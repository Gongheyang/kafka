/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Count;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsMetrics;

public class ProcessorNode<K, V> {

    private final List<ProcessorNode<?, ?>> children;

    private final String name;
    private final Processor<K, V> processor;
    protected NodeMetricsImpl nodeMetrics;
    private Time time;

    public final Set<String> stateStores;

    public ProcessorNode(String name) {
        this(name, null, null);
    }


    public ProcessorNode(String name, Processor<K, V> processor, Set<String> stateStores) {
        this.name = name;
        this.processor = processor;
        this.children = new ArrayList<>();
        this.stateStores = stateStores;
    }


    public final String name() {
        return name;
    }

    public final Processor<K, V> processor() {
        return processor;
    }

    public final List<ProcessorNode<?, ?>> children() {
        return children;
    }

    public void addChild(ProcessorNode<?, ?> child) {
        children.add(child);
    }


    public void init(ProcessorContext context, StreamsMetrics metrics) {
        try {
            processor.init(context);
        } catch (Exception e) {
            throw new StreamsException(String.format("failed to initialize processor %s", name), e);
        }
        this.time = time != null ? time : new SystemTime();
        this.nodeMetrics = new NodeMetricsImpl(metrics, name,  "task." + context.taskId());

    }

    public void close() {
        try {
            processor.close();
            nodeMetrics.removeAllSensors();
        } catch (Exception e) {
            throw new StreamsException(String.format("failed to close processor %s", name), e);
        }
    }

    public void process(final K key, final V value) {
        processor.process(key, value);
        nodeMetrics.nodeProcessTimeSensor.record();
    }

    /**
     * @return a string representation of this node, useful for debugging.
     */
    public String toString() {
        StringBuilder sb = new StringBuilder("");
        sb.append(name + ": ");
        if (stateStores != null && !stateStores.isEmpty()) {
            sb.append("stateStores [");
            for (String store : (Set<String>) stateStores) {
                sb.append(store + ",");
            }
            sb.setLength(sb.length() - 1);
            sb.append("] ");
        }
        return sb.toString();
    }

    protected class NodeMetricsImpl implements StreamsMetrics {
        final StreamsMetrics metrics;
        final String metricGrpName;
        final Map<String, String> metricTags;

        final Sensor nodeCommitTimeSensor;
        final Sensor nodePollTimeSensor;
        final Sensor nodeProcessTimeSensor;
        final Sensor nodePunctuateTimeSensor;
        final Sensor contextForwardSensor;
        final Sensor nodeTaskCreationSensor;
        final Sensor nodeTaskDestructionSensor;
        //TODO:
        //final Sensor nodeThroughputSensor;


        public NodeMetricsImpl(StreamsMetrics metrics, String name, String sensorNamePrefix) {
            this.metrics = metrics;
            this.metricGrpName = "node-metrics-" + name;
            this.metricTags = new LinkedHashMap<>();
            this.metricTags.put("node-id", "-" + name);

            this.nodeCommitTimeSensor = metrics.sensor(sensorNamePrefix + "node-commit-time-" + name);
            this.nodeCommitTimeSensor.add(new MetricName(sensorNamePrefix + "node-commit-time-avg", metricGrpName, "The average commit time in ms", metricTags), new Avg());
            this.nodeCommitTimeSensor.add(new MetricName(sensorNamePrefix + "node-commit-time-max", metricGrpName, "The maximum commit time in ms", metricTags), new Max());
            this.nodeCommitTimeSensor.add(new MetricName(sensorNamePrefix + "node-commit-calls-rate", metricGrpName, "The average per-second number of commit calls", metricTags), new Rate(new Count()));

            this.nodePollTimeSensor = metrics.sensor(sensorNamePrefix + "node-poll-time-" + name);
            this.nodePollTimeSensor.add(new MetricName(sensorNamePrefix + "node-poll-time-avg", metricGrpName, "The average poll time in ms", metricTags), new Avg());
            this.nodePollTimeSensor.add(new MetricName(sensorNamePrefix + "node-poll-time-max", metricGrpName, "The maximum poll time in ms", metricTags), new Max());
            this.nodePollTimeSensor.add(new MetricName(sensorNamePrefix + "node-poll-calls-rate", metricGrpName, "The average per-second number of record-poll calls", metricTags), new Rate(new Count()));

            this.nodeProcessTimeSensor = metrics.sensor(sensorNamePrefix + "node-process-time" + name);
            this.nodeProcessTimeSensor.add(new MetricName(sensorNamePrefix + "node-process-time-avg-ms", metricGrpName, "The average process time in ms", metricTags), new Avg());
            this.nodeProcessTimeSensor.add(new MetricName(sensorNamePrefix + "node-process-time-max-ms", metricGrpName, "The maximum process time in ms", metricTags), new Max());
            this.nodeProcessTimeSensor.add(new MetricName(sensorNamePrefix + "node-process-calls-rate", metricGrpName, "The average per-second number of process calls", metricTags), new Rate(new Count()));

            this.nodePunctuateTimeSensor = metrics.sensor(sensorNamePrefix + "node-punctuate-time" + name);
            this.nodePunctuateTimeSensor.add(new MetricName(sensorNamePrefix + "node-punctuate-time-avg", metricGrpName, "The average punctuate time in ms", metricTags), new Avg());
            this.nodePunctuateTimeSensor.add(new MetricName(sensorNamePrefix + "node-punctuate-time-max", metricGrpName, "The maximum punctuate time in ms", metricTags), new Max());
            this.nodePunctuateTimeSensor.add(new MetricName(sensorNamePrefix + "node-punctuate-calls-rate", metricGrpName, "The average per-second number of punctuate calls", metricTags), new Rate(new Count()));

            this.contextForwardSensor = metrics.sensor(sensorNamePrefix + "node-forward-time" + name);
            this.contextForwardSensor.add(new MetricName(sensorNamePrefix + "node-forward-creation-rate", metricGrpName, "The average per-second number of newly created tasks", metricTags), new Rate(new Count()));

            this.nodeTaskCreationSensor = metrics.sensor(sensorNamePrefix + "node-task-create-time" + name);
            this.nodeTaskCreationSensor.add(new MetricName(sensorNamePrefix + "node-task-create-time-avg", metricGrpName, "The average commit time in ms", metricTags), new Avg());
            this.nodeTaskCreationSensor.add(new MetricName(sensorNamePrefix + "node-task-create-time-max", metricGrpName, "The maximum commit time in ms", metricTags), new Max());
            this.nodeTaskCreationSensor.add(new MetricName(sensorNamePrefix + "node-task-create-rate", metricGrpName, "The average per-second number of commit calls", metricTags), new Rate(new Count()));

            this.nodeTaskDestructionSensor = metrics.sensor(sensorNamePrefix + "node-task-destruction" + name);
            this.nodeTaskDestructionSensor.add(new MetricName(sensorNamePrefix + "node-task-destruction-rate", metricGrpName, "The average per-second number of destructed tasks", metricTags), new Rate(new Count()));

        }

        public void removeAllSensors() {
            removeSensor(nodeCommitTimeSensor.name());
            removeSensor(nodePollTimeSensor.name());
            removeSensor(nodeProcessTimeSensor.name());
            removeSensor(nodePunctuateTimeSensor.name());
            removeSensor(contextForwardSensor.name());
            removeSensor(nodeTaskCreationSensor.name());
            removeSensor(nodeTaskDestructionSensor.name());
        }

        @Override
        public Sensor addLatencySensor(String scopeName, String entityName, String operationName, String... tags) {
            return metrics.addLatencySensor(scopeName, entityName, operationName, tags);
        }

        @Override
        public void recordLatency(Sensor sensor, long startNs, long endNs) {
            metrics.recordLatency(sensor, startNs, endNs);
        }

        @Override
        public Sensor sensor(String name) {
            return metrics.sensor(name);
        }

        @Override
        public Sensor addSensor(String name, Sensor... parents) {
            return metrics.addSensor(name, parents);
        }

        @Override
        public void removeSensor(String name) {
            metrics.removeSensor(name);
        }

        @Override
        public Sensor sensor(String name, MetricConfig config, Sensor... parents) {
            return metrics.sensor(name, config, parents);
        }

        @Override
        public Sensor getSensor(String name) {
            return metrics.getSensor(name);
        }
    }
}
