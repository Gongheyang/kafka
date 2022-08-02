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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.ConnectMetrics.MetricGroup;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.sink.SinkConnectorContext;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceConnectorContext;
import org.apache.kafka.connect.storage.CloseableOffsetStorageReader;
import org.apache.kafka.connect.storage.ConnectorOffsetBackingStore;
import org.apache.kafka.connect.util.Callback;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class WorkerConnectorTest {

    private static final String VERSION = "1.1";
    public static final String CONNECTOR = "connector";
    public static final Map<String, String> CONFIG = new HashMap<>();
    static {
        CONFIG.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, TestConnector.class.getName());
        CONFIG.put(ConnectorConfig.NAME_CONFIG, CONNECTOR);
        CONFIG.put(SinkConnectorConfig.TOPICS_CONFIG, "my-topic");
    }
    public ConnectorConfig connectorConfig;
    public MockConnectMetrics metrics;

    private final Plugins plugins = mock(Plugins.class);
    private final SourceConnector sourceConnector = mock(SourceConnector.class);
    private final SinkConnector sinkConnector = mock(SinkConnector.class);
    private final CloseableConnectorContext ctx = mock(CloseableConnectorContext.class);
    private final ConnectorStatus.Listener listener = mock(ConnectorStatus.Listener.class);
    private final CloseableOffsetStorageReader offsetStorageReader = mock(CloseableOffsetStorageReader.class);
    private final ConnectorOffsetBackingStore offsetStore = mock(ConnectorOffsetBackingStore.class);
    private final ClassLoader classLoader = mock(ClassLoader.class);
    private Connector connector;

    @Before
    public void setup() {
        connectorConfig = new ConnectorConfig(plugins, CONFIG);
        metrics = new MockConnectMetrics();
    }

    @After
    public void tearDown() {
        if (metrics != null) metrics.stop();
    }

    @Test
    public void testInitializeFailure() {
        RuntimeException exception = new RuntimeException();
        connector = sourceConnector;

        when(connector.version()).thenReturn(VERSION);
        doThrow(exception).when(connector).initialize(any());

        WorkerConnector workerConnector = new WorkerConnector(CONNECTOR, connector, connectorConfig, ctx, metrics, listener, offsetStorageReader, offsetStore, classLoader);

        workerConnector.initialize();
        assertFailedMetric(workerConnector);
        workerConnector.shutdown();
        workerConnector.doShutdown();
        assertStoppedMetric(workerConnector);

        verify(connector).version();
        verify(offsetStore).start();
        verify(connector).initialize(any(SourceConnectorContext.class));
        verify(listener).onFailure(CONNECTOR, exception);
        verify(listener).onShutdown(CONNECTOR);
        verify(ctx).close();
        verify(offsetStorageReader).close();
        verify(offsetStore).stop();
    }

    @Test
    public void testFailureIsFinalState() {
        RuntimeException exception = new RuntimeException();
        connector = sinkConnector;

        when(connector.version()).thenReturn(VERSION);
        doThrow(exception).when(connector).initialize(any());

        @SuppressWarnings("unchecked")
        Callback<TargetState> onStateChange = mock(Callback.class);
        WorkerConnector workerConnector = new WorkerConnector(CONNECTOR, connector, connectorConfig, ctx, metrics, listener, offsetStorageReader, offsetStore, classLoader);

        workerConnector.initialize();
        assertFailedMetric(workerConnector);
        workerConnector.doTransitionTo(TargetState.STARTED, onStateChange);
        assertFailedMetric(workerConnector);
        workerConnector.shutdown();
        workerConnector.doShutdown();
        assertStoppedMetric(workerConnector);

        verify(connector).version();
        verify(connector).initialize(any(SinkConnectorContext.class));
        verify(listener).onFailure(CONNECTOR, exception);
        // expect no call to onStartup() after failure
        verify(listener).onShutdown(CONNECTOR);
        verify(ctx).close();
        verify(offsetStorageReader).close();
        verify(offsetStore).stop();

        verify(onStateChange).onCompletion(any(Exception.class), isNull());
        verifyNoMoreInteractions(onStateChange);
    }

    @Test
    public void testStartupAndShutdown() {
        connector = sourceConnector;

        when(connector.version()).thenReturn(VERSION);

        @SuppressWarnings("unchecked")
        Callback<TargetState> onStateChange = mock(Callback.class);
        WorkerConnector workerConnector = new WorkerConnector(CONNECTOR, connector, connectorConfig, ctx, metrics, listener, offsetStorageReader, offsetStore, classLoader);

        workerConnector.initialize();
        assertInitializedSourceMetric(workerConnector);
        workerConnector.doTransitionTo(TargetState.STARTED, onStateChange);
        assertRunningMetric(workerConnector);
        workerConnector.shutdown();
        workerConnector.doShutdown();
        assertStoppedMetric(workerConnector);

        verify(connector).version();
        verify(offsetStore).start();
        verify(connector).initialize(any(SourceConnectorContext.class));
        verify(connector).start(CONFIG);
        verify(listener).onStartup(CONNECTOR);
        verify(connector).stop();
        verify(listener).onShutdown(CONNECTOR);
        verify(ctx).close();
        verify(offsetStorageReader).close();
        verify(offsetStore).stop();

        verify(onStateChange).onCompletion(isNull(), eq(TargetState.STARTED));
        verifyNoMoreInteractions(onStateChange);
    }

    @Test
    public void testStartupAndPause() {
        connector = sinkConnector;
        when(connector.version()).thenReturn(VERSION);

        @SuppressWarnings("unchecked")
        Callback<TargetState> onStateChange = mock(Callback.class);
        WorkerConnector workerConnector = new WorkerConnector(CONNECTOR, connector, connectorConfig, ctx, metrics, listener, offsetStorageReader, offsetStore, classLoader);

        workerConnector.initialize();
        assertInitializedSinkMetric(workerConnector);

        workerConnector.doTransitionTo(TargetState.STARTED, onStateChange);
        assertRunningMetric(workerConnector);
        workerConnector.doTransitionTo(TargetState.PAUSED, onStateChange);
        assertPausedMetric(workerConnector);
        workerConnector.shutdown();
        workerConnector.doShutdown();
        assertStoppedMetric(workerConnector);

        verify(connector).version();
        verify(connector).initialize(any(SinkConnectorContext.class));
        verify(connector).start(CONFIG);
        verify(listener).onStartup(CONNECTOR);
        verify(connector).stop();
        verify(listener).onPause(CONNECTOR);
        verify(listener).onShutdown(CONNECTOR);
        verify(ctx).close();
        verify(offsetStorageReader).close();
        verify(offsetStore).stop();

        InOrder inOrder = inOrder(onStateChange);
        inOrder.verify(onStateChange).onCompletion(isNull(), eq(TargetState.STARTED));
        inOrder.verify(onStateChange).onCompletion(isNull(), eq(TargetState.PAUSED));
        verifyNoMoreInteractions(onStateChange);
    }

    @Test
    public void testOnResume() {
        connector = sourceConnector;

        when(connector.version()).thenReturn(VERSION);

        @SuppressWarnings("unchecked")
        Callback<TargetState> onStateChange = mock(Callback.class);

        WorkerConnector workerConnector = new WorkerConnector(CONNECTOR, connector, connectorConfig, ctx, metrics, listener, offsetStorageReader, offsetStore, classLoader);

        workerConnector.initialize();
        assertInitializedSourceMetric(workerConnector);
        workerConnector.doTransitionTo(TargetState.PAUSED, onStateChange);
        assertPausedMetric(workerConnector);
        workerConnector.doTransitionTo(TargetState.STARTED, onStateChange);
        assertRunningMetric(workerConnector);
        workerConnector.shutdown();
        workerConnector.doShutdown();
        assertStoppedMetric(workerConnector);

        verify(connector).version();
        verify(connector).initialize(any(SourceConnectorContext.class));
        verify(offsetStore).start();
        verify(listener).onPause(CONNECTOR);
        verify(connector).start(CONFIG);
        verify(listener).onResume(CONNECTOR);
        verify(connector).stop();
        verify(listener).onShutdown(CONNECTOR);
        verify(ctx).close();
        verify(offsetStorageReader).close();
        verify(offsetStore).stop();

        InOrder inOrder = inOrder(onStateChange);
        inOrder.verify(onStateChange).onCompletion(isNull(), eq(TargetState.PAUSED));
        inOrder.verify(onStateChange).onCompletion(isNull(), eq(TargetState.STARTED));
        verifyNoMoreInteractions(onStateChange);
    }

    @Test
    public void testStartupPaused() {
        connector = sinkConnector;
        when(connector.version()).thenReturn(VERSION);

        @SuppressWarnings("unchecked")
        Callback<TargetState> onStateChange = mock(Callback.class);
        WorkerConnector workerConnector = new WorkerConnector(CONNECTOR, connector, connectorConfig, ctx, metrics, listener, offsetStorageReader, offsetStore, classLoader);

        workerConnector.initialize();
        assertInitializedSinkMetric(workerConnector);
        workerConnector.doTransitionTo(TargetState.PAUSED, onStateChange);
        assertPausedMetric(workerConnector);
        workerConnector.shutdown();
        workerConnector.doShutdown();
        assertStoppedMetric(workerConnector);

        verify(connector).version();
        verify(connector).initialize(any(SinkConnectorContext.class));
        // connector never gets started
        verify(listener).onPause(CONNECTOR);
        verify(listener).onShutdown(CONNECTOR);
        verify(ctx).close();
        verify(offsetStorageReader).close();
        verify(offsetStore).stop();

        verify(onStateChange).onCompletion(isNull(), eq(TargetState.PAUSED));
        verifyNoMoreInteractions(onStateChange);
    }

    @Test
    public void testStartupFailure() {
        RuntimeException exception = new RuntimeException();
        connector = sinkConnector;

        when(connector.version()).thenReturn(VERSION);
        doThrow(exception).when(connector).start(CONFIG);

        @SuppressWarnings("unchecked")
        Callback<TargetState> onStateChange = mock(Callback.class);
        WorkerConnector workerConnector = new WorkerConnector(CONNECTOR, connector, connectorConfig, ctx, metrics, listener, offsetStorageReader, offsetStore, classLoader);

        workerConnector.initialize();
        assertInitializedSinkMetric(workerConnector);
        workerConnector.doTransitionTo(TargetState.STARTED, onStateChange);
        assertFailedMetric(workerConnector);
        workerConnector.shutdown();
        workerConnector.doShutdown();
        assertStoppedMetric(workerConnector);

        verify(connector).version();
        verify(connector).initialize(any(SinkConnectorContext.class));
        verify(connector).start(CONFIG);
        verify(listener).onFailure(CONNECTOR, exception);
        verify(listener).onShutdown(CONNECTOR);
        verify(ctx).close();
        verify(offsetStorageReader).close();
        verify(offsetStore).stop();

        verify(onStateChange).onCompletion(any(Exception.class), isNull());
        verifyNoMoreInteractions(onStateChange);
    }

    @Test
    public void testShutdownFailure() {
        RuntimeException exception = new RuntimeException();
        connector = sourceConnector;

        when(connector.version()).thenReturn(VERSION);

        doThrow(exception).when(connector).stop();

        @SuppressWarnings("unchecked")
        Callback<TargetState> onStateChange = mock(Callback.class);
        WorkerConnector workerConnector = new WorkerConnector(CONNECTOR, connector, connectorConfig, ctx, metrics, listener, offsetStorageReader, offsetStore, classLoader);

        workerConnector.initialize();
        assertInitializedSourceMetric(workerConnector);
        workerConnector.doTransitionTo(TargetState.STARTED, onStateChange);
        assertRunningMetric(workerConnector);
        workerConnector.shutdown();
        workerConnector.doShutdown();
        assertFailedMetric(workerConnector);

        verify(connector).version();
        verify(offsetStore).start();
        verify(connector).initialize(any(SourceConnectorContext.class));
        verify(connector).start(CONFIG);
        verify(listener).onStartup(CONNECTOR);
        verify(connector).stop();
        verify(onStateChange).onCompletion(isNull(), eq(TargetState.STARTED));
        verifyNoMoreInteractions(onStateChange);
        verify(listener).onFailure(CONNECTOR, exception);
        verify(ctx).close();
        verify(offsetStorageReader).close();
        verify(offsetStore).stop();
    }

    @Test
    public void testTransitionStartedToStarted() {
        connector = sourceConnector;

        when(connector.version()).thenReturn(VERSION);

        @SuppressWarnings("unchecked")
        Callback<TargetState> onStateChange = mock(Callback.class);

        WorkerConnector workerConnector = new WorkerConnector(CONNECTOR, connector, connectorConfig, ctx, metrics, listener, offsetStorageReader, offsetStore, classLoader);

        workerConnector.initialize();
        assertInitializedSourceMetric(workerConnector);
        workerConnector.doTransitionTo(TargetState.STARTED, onStateChange);
        assertRunningMetric(workerConnector);
        workerConnector.doTransitionTo(TargetState.STARTED, onStateChange);
        assertRunningMetric(workerConnector);
        workerConnector.shutdown();
        workerConnector.doShutdown();
        assertStoppedMetric(workerConnector);

        verify(connector).version();
        verify(offsetStore).start();
        verify(connector).initialize(any(SourceConnectorContext.class));
        verify(connector).start(CONFIG);
        // expect only one call to onStartup()
        verify(listener).onStartup(CONNECTOR);
        verify(connector).stop();
        verify(listener).onShutdown(CONNECTOR);
        verify(ctx).close();
        verify(offsetStorageReader).close();
        verify(offsetStore).stop();
        verify(onStateChange, times(2)).onCompletion(isNull(), eq(TargetState.STARTED));
        verifyNoMoreInteractions(onStateChange);
    }

    @Test
    public void testTransitionPausedToPaused() {
        connector = sourceConnector;
        when(connector.version()).thenReturn(VERSION);

        @SuppressWarnings("unchecked")
        Callback<TargetState> onStateChange = mock(Callback.class);
        WorkerConnector workerConnector = new WorkerConnector(CONNECTOR, connector, connectorConfig, ctx, metrics, listener, offsetStorageReader, offsetStore, classLoader);

        workerConnector.initialize();
        assertInitializedSourceMetric(workerConnector);
        workerConnector.doTransitionTo(TargetState.STARTED, onStateChange);
        assertRunningMetric(workerConnector);
        workerConnector.doTransitionTo(TargetState.PAUSED, onStateChange);
        assertPausedMetric(workerConnector);
        workerConnector.doTransitionTo(TargetState.PAUSED, onStateChange);
        assertPausedMetric(workerConnector);
        workerConnector.shutdown();
        workerConnector.doShutdown();
        assertStoppedMetric(workerConnector);

        verify(connector).version();
        verify(offsetStore).start();
        verify(connector).initialize(any(SourceConnectorContext.class));
        verify(connector).start(CONFIG);
        verify(listener).onStartup(CONNECTOR);
        verify(connector).stop();
        verify(listener).onPause(CONNECTOR);
        verify(listener).onShutdown(CONNECTOR);
        verify(ctx).close();
        verify(offsetStorageReader).close();
        verify(offsetStore).stop();

        InOrder inOrder = inOrder(onStateChange);
        inOrder.verify(onStateChange).onCompletion(isNull(), eq(TargetState.STARTED));
        inOrder.verify(onStateChange, times(2)).onCompletion(isNull(), eq(TargetState.PAUSED));
        verifyNoMoreInteractions(onStateChange);
    }

    @Test
    public void testFailConnectorThatIsNeitherSourceNorSink() {
        connector = mock(Connector.class);
        when(connector.version()).thenReturn(VERSION);
        WorkerConnector workerConnector = new WorkerConnector(CONNECTOR, connector, connectorConfig, ctx, metrics, listener, offsetStorageReader, offsetStore, classLoader);

        workerConnector.initialize();

        verify(connector).version();
        ArgumentCaptor<Throwable> exceptionCapture = ArgumentCaptor.forClass(Throwable.class);
        verify(listener).onFailure(eq(CONNECTOR), exceptionCapture.capture());
        Throwable e = exceptionCapture.getValue();
        assertTrue(e instanceof ConnectException);
        assertTrue(e.getMessage().contains("must be a subclass of"));
    }

    protected void assertFailedMetric(WorkerConnector workerConnector) {
        assertFalse(workerConnector.metrics().isUnassigned());
        assertTrue(workerConnector.metrics().isFailed());
        assertFalse(workerConnector.metrics().isPaused());
        assertFalse(workerConnector.metrics().isRunning());
    }

    protected void assertPausedMetric(WorkerConnector workerConnector) {
        assertFalse(workerConnector.metrics().isUnassigned());
        assertFalse(workerConnector.metrics().isFailed());
        assertTrue(workerConnector.metrics().isPaused());
        assertFalse(workerConnector.metrics().isRunning());
    }

    protected void assertRunningMetric(WorkerConnector workerConnector) {
        assertFalse(workerConnector.metrics().isUnassigned());
        assertFalse(workerConnector.metrics().isFailed());
        assertFalse(workerConnector.metrics().isPaused());
        assertTrue(workerConnector.metrics().isRunning());
    }

    protected void assertStoppedMetric(WorkerConnector workerConnector) {
        assertTrue(workerConnector.metrics().isUnassigned());
        assertFalse(workerConnector.metrics().isFailed());
        assertFalse(workerConnector.metrics().isPaused());
        assertFalse(workerConnector.metrics().isRunning());
    }

    protected void assertInitializedSinkMetric(WorkerConnector workerConnector) {
        assertInitializedMetric(workerConnector, "sink");
    }

    protected void assertInitializedSourceMetric(WorkerConnector workerConnector) {
        assertInitializedMetric(workerConnector, "source");
    }

    protected void assertInitializedMetric(WorkerConnector workerConnector, String expectedType) {
        assertTrue(workerConnector.metrics().isUnassigned());
        assertFalse(workerConnector.metrics().isFailed());
        assertFalse(workerConnector.metrics().isPaused());
        assertFalse(workerConnector.metrics().isRunning());
        MetricGroup metricGroup = workerConnector.metrics().metricGroup();
        String status = metrics.currentMetricValueAsString(metricGroup, "status");
        String type = metrics.currentMetricValueAsString(metricGroup, "connector-type");
        String clazz = metrics.currentMetricValueAsString(metricGroup, "connector-class");
        String version = metrics.currentMetricValueAsString(metricGroup, "connector-version");
        assertEquals(expectedType, type);
        assertNotNull(clazz);
        assertEquals(VERSION, version);
    }

    private static abstract class TestConnector extends Connector {
    }
}
