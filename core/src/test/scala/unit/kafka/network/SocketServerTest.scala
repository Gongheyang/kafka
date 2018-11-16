/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.network

import java.io._
import java.net._
import java.nio.ByteBuffer
import java.nio.channels.{SelectionKey, SocketChannel}
import java.util
import java.util.concurrent.{CompletableFuture, ConcurrentLinkedQueue, Executors, TimeUnit}
import java.util.{HashMap, Properties, Random}

import com.yammer.metrics.core.{Gauge, Meter}
import com.yammer.metrics.{Metrics => YammerMetrics}
import javax.net.ssl._

import kafka.security.CredentialProvider
import kafka.server.{KafkaConfig, ThrottledChannel}
import kafka.utils.Implicits._
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.message.SaslAuthenticateRequestData
import org.apache.kafka.common.message.SaslHandshakeRequestData
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.ClientInformation
import org.apache.kafka.common.network.KafkaChannel.ChannelMuteState
import org.apache.kafka.common.network._
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.requests.{AbstractRequest, ApiVersionsRequest, ProduceRequest, RequestHeader, SaslAuthenticateRequest, SaslHandshakeRequest}
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.common.security.scram.internals.ScramMechanism
import org.apache.kafka.common.utils.AppInfoParser
import org.apache.kafka.common.utils.{LogContext, MockTime, Time}
import org.apache.kafka.test.{TestSslUtils, TestUtils => JTestUtils}
import org.apache.log4j.Level
import org.junit.Assert._
import org.junit._
import org.scalatest.Assertions.fail

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.ControlThrowable

class SocketServerTest {
  val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 0)
  props.put("listeners", "PLAINTEXT://localhost:0")
  props.put("num.network.threads", "1")
  props.put("socket.send.buffer.bytes", "300000")
  props.put("socket.receive.buffer.bytes", "300000")
  props.put("queued.max.requests", "50")
  props.put("socket.request.max.bytes", "100")
  props.put("max.connections.per.ip", "5")
  props.put("connections.max.idle.ms", "60000")
  val config = KafkaConfig.fromProps(props)
  val metrics = new Metrics
  val credentialProvider = new CredentialProvider(ScramMechanism.mechanismNames, null)
  val localAddress = InetAddress.getLoopbackAddress

  // Clean-up any metrics left around by previous tests
  TestUtils.clearYammerMetrics()

  val server = new SocketServer(config, metrics, Time.SYSTEM, credentialProvider)
  server.startup()
  val sockets = new ArrayBuffer[Socket]

  private val kafkaLogger = org.apache.log4j.LogManager.getLogger("kafka")
  private var logLevelToRestore: Level = _

  @Before
  def setUp(): Unit = {
    // Run the tests with TRACE logging to exercise request logging path
    logLevelToRestore = kafkaLogger.getLevel
    kafkaLogger.setLevel(Level.TRACE)
  }

  @After
  def tearDown(): Unit = {
    shutdownServerAndMetrics(server)
    sockets.foreach(_.close())
    sockets.clear()
    kafkaLogger.setLevel(logLevelToRestore)
  }

  def sendRequest(socket: Socket, request: Array[Byte], id: Option[Short] = None, flush: Boolean = true): Unit = {
    val outgoing = new DataOutputStream(socket.getOutputStream)
    id match {
      case Some(id) =>
        outgoing.writeInt(request.length + 2)
        outgoing.writeShort(id)
      case None =>
        outgoing.writeInt(request.length)
    }
    outgoing.write(request)
    if (flush)
      outgoing.flush()
  }

  def sendApiRequest(socket: Socket, request: AbstractRequest, header: RequestHeader) = {
    val byteBuffer = request.serialize(header)
    byteBuffer.rewind()
    val serializedBytes = new Array[Byte](byteBuffer.remaining)
    byteBuffer.get(serializedBytes)
    sendRequest(socket, serializedBytes)
  }

  def receiveResponse(socket: Socket): Array[Byte] = {
    val incoming = new DataInputStream(socket.getInputStream)
    val len = incoming.readInt()
    val response = new Array[Byte](len)
    incoming.readFully(response)
    response
  }

  private def receiveRequest(channel: RequestChannel, timeout: Long = 2000L): RequestChannel.Request = {
    channel.receiveRequest(timeout) match {
      case request: RequestChannel.Request => request
      case RequestChannel.ShutdownRequest => fail("Unexpected shutdown received")
      case null => fail("receiveRequest timed out")
    }
  }

  /* A simple request handler that just echos back the response */
  def processRequest(channel: RequestChannel): Unit = {
    processRequest(channel, receiveRequest(channel))
  }

  def processRequest(channel: RequestChannel, request: RequestChannel.Request): Unit = {
    val byteBuffer = request.body[AbstractRequest].serialize(request.header)
    byteBuffer.rewind()

    val send = new NetworkSend(request.context.connectionId, byteBuffer)
    channel.sendResponse(new RequestChannel.SendResponse(request, send, Some(request.header.toString), None))
  }

  def processRequestNoOpResponse(channel: RequestChannel, request: RequestChannel.Request): Unit = {
    channel.sendResponse(new RequestChannel.NoOpResponse(request))
  }

  def connect(s: SocketServer = server,
              listenerName: ListenerName = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
              localAddr: InetAddress = null,
              port: Int = 0): Socket = {
    val socket = new Socket("localhost", s.boundPort(listenerName), localAddr, port)
    sockets += socket
    socket
  }

  def sslConnect(s: SocketServer = server): Socket = {
    val socket = sslClientSocket(s.boundPort(ListenerName.forSecurityProtocol(SecurityProtocol.SSL)))
    sockets += socket
    socket
  }

  private def sslClientSocket(port: Int): Socket = {
    val sslContext = SSLContext.getInstance("TLSv1.2")
    sslContext.init(null, Array(TestUtils.trustAllCerts), new java.security.SecureRandom())
    val socketFactory = sslContext.getSocketFactory
    val socket = socketFactory.createSocket("localhost", port)
    socket.asInstanceOf[SSLSocket].setNeedClientAuth(false)
    socket
  }

  // Create a client connection, process one request and return (client socket, connectionId)
  def connectAndProcessRequest(s: SocketServer): (Socket, String) = {
    val securityProtocol = s.dataPlaneAcceptors.asScala.head._1.securityProtocol
    val socket = securityProtocol match {
      case SecurityProtocol.PLAINTEXT | SecurityProtocol.SASL_PLAINTEXT =>
        connect(s)
      case SecurityProtocol.SSL | SecurityProtocol.SASL_SSL =>
        sslConnect(s)
      case _ =>
        throw new IllegalStateException(s"Unexpected security protocol $securityProtocol")
    }
    val request = sendAndReceiveRequest(socket, s)
    processRequest(s.dataPlaneRequestChannel, request)
    (socket, request.context.connectionId)
  }

  def sendAndReceiveRequest(socket: Socket, server: SocketServer): RequestChannel.Request = {
    sendRequest(socket, producerRequestBytes())
    receiveRequest(server.dataPlaneRequestChannel)
  }

  def shutdownServerAndMetrics(server: SocketServer): Unit = {
    server.shutdown()
    server.metrics.close()
  }

  private def producerRequestBytes(ack: Short = 0): Array[Byte] = {
    val correlationId = -1
    val clientId = ""
    val ackTimeoutMs = 10000

    val emptyRequest = ProduceRequest.Builder.forCurrentMagic(ack, ackTimeoutMs,
      new HashMap[TopicPartition, MemoryRecords]()).build()
    val emptyHeader = new RequestHeader(ApiKeys.PRODUCE, emptyRequest.version, clientId, correlationId)
    val byteBuffer = emptyRequest.serialize(emptyHeader)
    byteBuffer.rewind()

    val serializedBytes = new Array[Byte](byteBuffer.remaining)
    byteBuffer.get(serializedBytes)
    serializedBytes
  }

  private def apiVersionRequestBytes(clientId: String, version: Short): Array[Byte] = {
    val request = new ApiVersionsRequest.Builder().build(version)
    val header = new RequestHeader(ApiKeys.API_VERSIONS, request.version(), clientId, -1)
    val buffer = request.serialize(header)
    buffer.rewind()
    val bytes = new Array[Byte](buffer.remaining())
    buffer.get(bytes)
    bytes
  }

  @Test
  def simpleRequest(): Unit = {
    val plainSocket = connect()
    val serializedBytes = producerRequestBytes()

    // Test PLAINTEXT socket
    sendRequest(plainSocket, serializedBytes)
    processRequest(server.dataPlaneRequestChannel)
    assertEquals(serializedBytes.toSeq, receiveResponse(plainSocket).toSeq)
    verifyAcceptorBlockedPercent("PLAINTEXT", expectBlocked = false)
  }


  private def testClientInformation(version: Short, expectedClientSoftwareName: String,
                                    expectedClientSoftwareVersion: String): Unit = {
    val plainSocket = connect()
    val address = plainSocket.getLocalAddress
    val clientId = "clientId"

    // Send ApiVersionsRequest - unknown expected
    sendRequest(plainSocket, apiVersionRequestBytes(clientId, version))
    var receivedReq = receiveRequest(server.dataPlaneRequestChannel)

    assertEquals(ClientInformation.UNKNOWN_NAME_OR_VERSION, receivedReq.context.clientInformation.softwareName)
    assertEquals(ClientInformation.UNKNOWN_NAME_OR_VERSION, receivedReq.context.clientInformation.softwareVersion)

    server.dataPlaneRequestChannel.sendResponse(new RequestChannel.NoOpResponse(receivedReq))

    // Send ProduceRequest - client info expected
    sendRequest(plainSocket, producerRequestBytes())
    receivedReq = receiveRequest(server.dataPlaneRequestChannel)

    assertEquals(expectedClientSoftwareName, receivedReq.context.clientInformation.softwareName)
    assertEquals(expectedClientSoftwareVersion, receivedReq.context.clientInformation.softwareVersion)

    server.dataPlaneRequestChannel.sendResponse(new RequestChannel.NoOpResponse(receivedReq))

    // Close the socket
    plainSocket.setSoLinger(true, 0)
    plainSocket.close()

    TestUtils.waitUntilTrue(() => server.connectionCount(address) == 0, msg = "Connection not closed")
  }

  @Test
  def testClientInformationWithLatestApiVersionsRequest(): Unit = {
    testClientInformation(
      ApiKeys.API_VERSIONS.latestVersion,
      "apache-kafka-java",
      AppInfoParser.getVersion
    )
  }

  @Test
  def testClientInformationWithOldestApiVersionsRequest(): Unit = {
    testClientInformation(
      ApiKeys.API_VERSIONS.oldestVersion,
      ClientInformation.UNKNOWN_NAME_OR_VERSION,
      ClientInformation.UNKNOWN_NAME_OR_VERSION
    )
  }

  @Test
  def testStagedListenerStartup(): Unit = {
    val testProps = new Properties
    testProps ++= props
    testProps.put("listeners", "EXTERNAL://localhost:0,INTERNAL://localhost:0,CONTROLLER://localhost:0")
    testProps.put("listener.security.protocol.map", "EXTERNAL:PLAINTEXT,INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT")
    testProps.put("control.plane.listener.name", "CONTROLLER")
    testProps.put("inter.broker.listener.name", "INTERNAL")
    val config = KafkaConfig.fromProps(testProps)
    val testableServer = new TestableSocketServer(config)
    testableServer.startup(startupProcessors = false)
    val updatedEndPoints = config.advertisedListeners.map { endpoint =>
      endpoint.copy(port = testableServer.boundPort(endpoint.listenerName))
    }.map(_.toJava)

    val externalReadyFuture = new CompletableFuture[Void]()
    val executor = Executors.newSingleThreadExecutor()

    def listenerStarted(listenerName: ListenerName) = {
      try {
        val socket = connect(testableServer, listenerName, localAddr = InetAddress.getLocalHost)
        sendAndReceiveRequest(socket, testableServer)
        true
      } catch {
        case _: Throwable => false
      }
    }

    try {
      testableServer.startControlPlaneProcessor()
      val socket1 = connect(testableServer, config.controlPlaneListenerName.get, localAddr = InetAddress.getLocalHost)
      sendAndReceiveControllerRequest(socket1, testableServer)

      val externalListener = new ListenerName("EXTERNAL")
      val externalEndpoint = updatedEndPoints.find(e => e.listenerName.get == externalListener.value).get
      val futures =  Map(externalEndpoint -> externalReadyFuture)
      val startFuture = executor.submit((() => testableServer.startDataPlaneProcessors(futures)): Runnable)
      TestUtils.waitUntilTrue(() => listenerStarted(config.interBrokerListenerName), "Inter-broker listener not started")
      assertFalse("Socket server startup did not wait for future to complete", startFuture.isDone)

      assertFalse(listenerStarted(externalListener))

      externalReadyFuture.complete(null)
      TestUtils.waitUntilTrue(() => listenerStarted(externalListener), "External listener not started")
    } finally {
      executor.shutdownNow()
      shutdownServerAndMetrics(testableServer)
    }
  }

  @Test
  def testControlPlaneRequest(): Unit = {
    val testProps = new Properties
    testProps ++= props
    testProps.put("listeners", "PLAINTEXT://localhost:0,CONTROLLER://localhost:0")
    testProps.put("listener.security.protocol.map", "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT")
    testProps.put("control.plane.listener.name", "CONTROLLER")
    val config = KafkaConfig.fromProps(testProps)
    withTestableServer(config, { testableServer =>
      val socket = connect(testableServer, config.controlPlaneListenerName.get,
        localAddr = InetAddress.getLocalHost)
      sendAndReceiveControllerRequest(socket, testableServer)
    })
  }

  @Test
  def tooBigRequestIsRejected(): Unit = {
    val tooManyBytes = new Array[Byte](server.config.socketRequestMaxBytes + 1)
    new Random().nextBytes(tooManyBytes)
    val socket = connect()
    val outgoing = new DataOutputStream(socket.getOutputStream)
    outgoing.writeInt(tooManyBytes.length)
    try {
      // Server closes client connection when it processes the request length because
      // it is too big. The write of request body may fail if the connection has been closed.
      outgoing.write(tooManyBytes)
      outgoing.flush()
      receiveResponse(socket)
    } catch {
      case _: IOException => // thats fine
    }
  }

  @Test
  def testGracefulClose(): Unit = {
    val plainSocket = connect()
    val serializedBytes = producerRequestBytes()

    for (_ <- 0 until 10)
      sendRequest(plainSocket, serializedBytes)
    plainSocket.close()
    for (_ <- 0 until 10) {
      val request = receiveRequest(server.dataPlaneRequestChannel)
      assertNotNull("receiveRequest timed out", request)
      processRequestNoOpResponse(server.dataPlaneRequestChannel, request)
    }
  }

  @Test
  def testNoOpAction(): Unit = {
    val plainSocket = connect()
    val serializedBytes = producerRequestBytes()

    for (_ <- 0 until 3)
      sendRequest(plainSocket, serializedBytes)
    for (_ <- 0 until 3) {
      val request = receiveRequest(server.dataPlaneRequestChannel)
      assertNotNull("receiveRequest timed out", request)
      processRequestNoOpResponse(server.dataPlaneRequestChannel, request)
    }
  }

  @Test
  def testConnectionId(): Unit = {
    val sockets = (1 to 5).map(_ => connect())
    val serializedBytes = producerRequestBytes()

    val requests = sockets.map{socket =>
      sendRequest(socket, serializedBytes)
      receiveRequest(server.dataPlaneRequestChannel)
    }
    requests.zipWithIndex.foreach { case (request, i) =>
      val index = request.context.connectionId.split("-").last
      assertEquals(i.toString, index)
    }

    sockets.foreach(_.close)
  }

  @Test
  def testIdleConnection(): Unit = {
    val idleTimeMs = 60000
    val time = new MockTime()
    props.put(KafkaConfig.ConnectionsMaxIdleMsProp, idleTimeMs.toString)
    val serverMetrics = new Metrics
    val overrideServer = new SocketServer(KafkaConfig.fromProps(props), serverMetrics, time, credentialProvider)

    try {
      overrideServer.startup()
      val serializedBytes = producerRequestBytes()

      // Connection with no outstanding requests
      val socket0 = connect(overrideServer)
      sendRequest(socket0, serializedBytes)
      val request0 = receiveRequest(overrideServer.dataPlaneRequestChannel)
      processRequest(overrideServer.dataPlaneRequestChannel, request0)
      assertTrue("Channel not open", openChannel(request0, overrideServer).nonEmpty)
      assertEquals(openChannel(request0, overrideServer), openOrClosingChannel(request0, overrideServer))
      TestUtils.waitUntilTrue(() => !openChannel(request0, overrideServer).head.isMuted, "Failed to unmute channel")
      time.sleep(idleTimeMs + 1)
      TestUtils.waitUntilTrue(() => openOrClosingChannel(request0, overrideServer).isEmpty, "Failed to close idle channel")
      assertTrue("Channel not removed", openChannel(request0, overrideServer).isEmpty)

      // Connection with one request being processed (channel is muted), no other in-flight requests
      val socket1 = connect(overrideServer)
      sendRequest(socket1, serializedBytes)
      val request1 = receiveRequest(overrideServer.dataPlaneRequestChannel)
      assertTrue("Channel not open", openChannel(request1, overrideServer).nonEmpty)
      assertEquals(openChannel(request1, overrideServer), openOrClosingChannel(request1, overrideServer))
      time.sleep(idleTimeMs + 1)
      TestUtils.waitUntilTrue(() => openOrClosingChannel(request1, overrideServer).isEmpty, "Failed to close idle channel")
      assertTrue("Channel not removed", openChannel(request1, overrideServer).isEmpty)
      processRequest(overrideServer.dataPlaneRequestChannel, request1)

      // Connection with one request being processed (channel is muted), more in-flight requests
      val socket2 = connect(overrideServer)
      val request2 = sendRequestsReceiveOne(overrideServer, socket2, serializedBytes, 3)
      time.sleep(idleTimeMs + 1)
      TestUtils.waitUntilTrue(() => openOrClosingChannel(request2, overrideServer).isEmpty, "Failed to close idle channel")
      assertTrue("Channel not removed", openChannel(request1, overrideServer).isEmpty)
      processRequest(overrideServer.dataPlaneRequestChannel, request2) // this triggers a failed send since channel has been closed
      assertNull("Received request on expired channel", overrideServer.dataPlaneRequestChannel.receiveRequest(200))

    } finally {
      shutdownServerAndMetrics(overrideServer)
    }
  }

  @Test
  def testConnectionIdReuse(): Unit = {
    val idleTimeMs = 60000
    val time = new MockTime()
    props.put(KafkaConfig.ConnectionsMaxIdleMsProp, idleTimeMs.toString)
    props ++= sslServerProps
    val serverMetrics = new Metrics
    @volatile var selector: TestableSelector = null
    val overrideConnectionId = "127.0.0.1:1-127.0.0.1:2-0"
    val overrideServer = new SocketServer(KafkaConfig.fromProps(props), serverMetrics, time, credentialProvider) {
      override def newProcessor(id: Int, requestChannel: RequestChannel, connectionQuotas: ConnectionQuotas, listenerName: ListenerName,
                                protocol: SecurityProtocol, memoryPool: MemoryPool): Processor = {
        new Processor(id, time, config.socketRequestMaxBytes, dataPlaneRequestChannel, connectionQuotas,
          config.connectionsMaxIdleMs, config.failedAuthenticationDelayMs, listenerName, protocol, config, metrics,
          credentialProvider, memoryPool, new LogContext()) {
            override protected[network] def connectionId(socket: Socket): String = overrideConnectionId
            override protected[network] def createSelector(channelBuilder: ChannelBuilder): Selector = {
             val testableSelector = new TestableSelector(config, channelBuilder, time, metrics)
             selector = testableSelector
             testableSelector
          }
        }
      }
    }

    def openChannel: Option[KafkaChannel] = overrideServer.dataPlaneProcessor(0).channel(overrideConnectionId)
    def openOrClosingChannel: Option[KafkaChannel] = overrideServer.dataPlaneProcessor(0).openOrClosingChannel(overrideConnectionId)
    def connectionCount = overrideServer.connectionCount(InetAddress.getByName("127.0.0.1"))

    // Create a client connection and wait for server to register the connection with the selector. For
    // test scenarios below where `Selector.register` fails, the wait ensures that checks are performed
    // only after `register` is processed by the server.
    def connectAndWaitForConnectionRegister(): Socket = {
      val connections = selector.operationCounts(SelectorOperation.Register)
      val socket = sslConnect(overrideServer)
      TestUtils.waitUntilTrue(() =>
        selector.operationCounts(SelectorOperation.Register) == connections + 1, "Connection not registered")
      socket
    }

    try {
      overrideServer.startup()
      val socket1 = connectAndWaitForConnectionRegister()
      TestUtils.waitUntilTrue(() => connectionCount == 1 && openChannel.isDefined, "Failed to create channel")
      val channel1 = openChannel.getOrElse(throw new RuntimeException("Channel not found"))

      // Create new connection with same id when `channel1` is still open and in Selector.channels
      // Check that new connection is closed and openChannel still contains `channel1`
      connectAndWaitForConnectionRegister()
      TestUtils.waitUntilTrue(() => connectionCount == 1, "Failed to close channel")
      assertSame(channel1, openChannel.getOrElse(throw new RuntimeException("Channel not found")))
      socket1.close()
      TestUtils.waitUntilTrue(() => openChannel.isEmpty, "Channel not closed")

      // Create a channel with buffered receive and close remote connection
      val request = makeChannelWithBufferedRequestsAndCloseRemote(overrideServer, selector)
      val channel2 = openChannel.getOrElse(throw new RuntimeException("Channel not found"))

      // Create new connection with same id when `channel2` is closing, but still in Selector.channels
      // Check that new connection is closed and openOrClosingChannel still contains `channel2`
      connectAndWaitForConnectionRegister()
      TestUtils.waitUntilTrue(() => connectionCount == 1, "Failed to close channel")
      assertSame(channel2, openOrClosingChannel.getOrElse(throw new RuntimeException("Channel not found")))

      // Complete request with failed send so that `channel2` is removed from Selector.channels
      processRequest(overrideServer.dataPlaneRequestChannel, request)
      TestUtils.waitUntilTrue(() => connectionCount == 0 && openOrClosingChannel.isEmpty, "Failed to remove channel with failed send")

      // Check that new connections can be created with the same id since `channel1` is no longer in Selector
      connectAndWaitForConnectionRegister()
      TestUtils.waitUntilTrue(() => connectionCount == 1 && openChannel.isDefined, "Failed to open new channel")
      val newChannel = openChannel.getOrElse(throw new RuntimeException("Channel not found"))
      assertNotSame(channel1, newChannel)
      newChannel.disconnect()

    } finally {
      shutdownServerAndMetrics(overrideServer)
    }
  }

  private def makeSocketWithBufferedRequests(server: SocketServer,
                                             serverSelector: Selector,
                                             proxyServer: ProxyServer,
                                             numBufferedRequests: Int = 2): (Socket, RequestChannel.Request) = {

    val requestBytes = producerRequestBytes()
    val socket = sslClientSocket(proxyServer.localPort)
    sendRequest(socket, requestBytes)
    val request1 = receiveRequest(server.dataPlaneRequestChannel)

    val connectionId = request1.context.connectionId
    val channel = server.dataPlaneProcessor(0).channel(connectionId).getOrElse(throw new IllegalStateException("Channel not found"))
    val transportLayer: SslTransportLayer = JTestUtils.fieldValue(channel, classOf[KafkaChannel], "transportLayer")
    val netReadBuffer: ByteBuffer = JTestUtils.fieldValue(transportLayer, classOf[SslTransportLayer], "netReadBuffer")

    proxyServer.enableBuffering(netReadBuffer)
    (1 to numBufferedRequests).foreach { _ => sendRequest(socket, requestBytes) }

    val keysWithBufferedRead: util.Set[SelectionKey] = JTestUtils.fieldValue(serverSelector, classOf[Selector], "keysWithBufferedRead")
    keysWithBufferedRead.add(channel.selectionKey)
    JTestUtils.setFieldValue(transportLayer, "hasBytesBuffered", true)

    (socket, request1)
  }

  /**
   * Create a channel with data in SSL buffers and close the remote connection.
   * The channel should remain open in SocketServer even if it detects that the peer has closed
   * the connection since there is pending data to be processed.
   */
  private def makeChannelWithBufferedRequestsAndCloseRemote(server: SocketServer,
                                                            serverSelector: Selector,
                                                            makeClosing: Boolean = false): RequestChannel.Request = {

    val proxyServer = new ProxyServer(server)
    try {
      val (socket, request1) = makeSocketWithBufferedRequests(server, serverSelector, proxyServer)

      socket.close()
      proxyServer.serverConnSocket.close()
      TestUtils.waitUntilTrue(() => proxyServer.clientConnSocket.isClosed, "Client socket not closed")

      processRequestNoOpResponse(server.dataPlaneRequestChannel, request1)
      val channel = openOrClosingChannel(request1, server).getOrElse(throw new IllegalStateException("Channel closed too early"))
      if (makeClosing)
        serverSelector.asInstanceOf[TestableSelector].pendingClosingChannels.add(channel)

      receiveRequest(server.dataPlaneRequestChannel, timeout = 10000)
    } finally {
      proxyServer.close()
    }
  }

  def sendRequestsReceiveOne(server: SocketServer, socket: Socket, requestBytes: Array[Byte], numRequests: Int): RequestChannel.Request = {
    (1 to numRequests).foreach(i => sendRequest(socket, requestBytes, flush = i == numRequests))
    receiveRequest(server.dataPlaneRequestChannel)
  }

  private def closeSocketWithPendingRequest(server: SocketServer,
                                            createSocket: () => Socket): RequestChannel.Request = {

    def maybeReceiveRequest(): Option[RequestChannel.Request] = {
      try {
        Some(receiveRequest(server.dataPlaneRequestChannel, timeout = 1000))
      } catch {
        case e: Exception => None
      }
    }

    def closedChannelWithPendingRequest(): Option[RequestChannel.Request] = {
      val socket = createSocket.apply()
      val req1 = sendRequestsReceiveOne(server, socket, producerRequestBytes(ack = 0), numRequests = 100)
      processRequestNoOpResponse(server.dataPlaneRequestChannel, req1)
      // Set SoLinger to 0 to force a hard disconnect via TCP RST
      socket.setSoLinger(true, 0)
      socket.close()

      maybeReceiveRequest().flatMap { req =>
        processRequestNoOpResponse(server.dataPlaneRequestChannel, req)
        maybeReceiveRequest()
      }
    }

    val (request, _) = TestUtils.computeUntilTrue(closedChannelWithPendingRequest()) { req => req.nonEmpty }
    request.getOrElse(throw new IllegalStateException("Could not create close channel with pending request"))
  }

  // Prepares test setup for throttled channel tests. throttlingDone controls whether or not throttling has completed
  // in quota manager.
  def throttledChannelTestSetUp(socket: Socket, serializedBytes: Array[Byte], noOpResponse: Boolean,
                                throttlingInProgress: Boolean): RequestChannel.Request = {
    sendRequest(socket, serializedBytes)

    // Mimic a primitive request handler that fetches the request from RequestChannel and place a response with a
    // throttled channel.
    val request = receiveRequest(server.dataPlaneRequestChannel)
    val byteBuffer = request.body[AbstractRequest].serialize(request.header)
    val send = new NetworkSend(request.context.connectionId, byteBuffer)
    def channelThrottlingCallback(response: RequestChannel.Response): Unit = {
      server.dataPlaneRequestChannel.sendResponse(response)
    }
    val throttledChannel = new ThrottledChannel(request, new MockTime(), 100, channelThrottlingCallback)
    val response =
      if (!noOpResponse)
        new RequestChannel.SendResponse(request, send, Some(request.header.toString), None)
      else
        new RequestChannel.NoOpResponse(request)
    server.dataPlaneRequestChannel.sendResponse(response)

    // Quota manager would call notifyThrottlingDone() on throttling completion. Simulate it if throttleingInProgress is
    // false.
    if (!throttlingInProgress)
      throttledChannel.notifyThrottlingDone()

    request
  }

  def openChannel(request: RequestChannel.Request, server: SocketServer = this.server): Option[KafkaChannel] =
    server.dataPlaneProcessor(0).channel(request.context.connectionId)

  def openOrClosingChannel(request: RequestChannel.Request, server: SocketServer = this.server): Option[KafkaChannel] =
    server.dataPlaneProcessor(0).openOrClosingChannel(request.context.connectionId)

  @Test
  def testSendActionResponseWithThrottledChannelWhereThrottlingInProgress(): Unit = {
    val socket = connect()
    val serializedBytes = producerRequestBytes()
    // SendAction with throttling in progress
    val request = throttledChannelTestSetUp(socket, serializedBytes, false, true)

    // receive response
    assertEquals(serializedBytes.toSeq, receiveResponse(socket).toSeq)
    TestUtils.waitUntilTrue(() => openOrClosingChannel(request).exists(c => c.muteState() == ChannelMuteState.MUTED_AND_THROTTLED), "fail")
    // Channel should still be muted.
    assertTrue(openOrClosingChannel(request).exists(c => c.isMuted()))
  }

  @Test
  def testSendActionResponseWithThrottledChannelWhereThrottlingAlreadyDone(): Unit = {
    val socket = connect()
    val serializedBytes = producerRequestBytes()
    // SendAction with throttling in progress
    val request = throttledChannelTestSetUp(socket, serializedBytes, false, false)

    // receive response
    assertEquals(serializedBytes.toSeq, receiveResponse(socket).toSeq)
    // Since throttling is already done, the channel can be unmuted after sending out the response.
    TestUtils.waitUntilTrue(() => openOrClosingChannel(request).exists(c => c.muteState() == ChannelMuteState.NOT_MUTED), "fail")
    // Channel is now unmuted.
    assertFalse(openOrClosingChannel(request).exists(c => c.isMuted()))
  }

  @Test
  def testNoOpActionResponseWithThrottledChannelWhereThrottlingInProgress(): Unit = {
    val socket = connect()
    val serializedBytes = producerRequestBytes()
    // SendAction with throttling in progress
    val request = throttledChannelTestSetUp(socket, serializedBytes, true, true)

    TestUtils.waitUntilTrue(() => openOrClosingChannel(request).exists(c => c.muteState() == ChannelMuteState.MUTED_AND_THROTTLED), "fail")
    // Channel should still be muted.
    assertTrue(openOrClosingChannel(request).exists(c => c.isMuted()))
  }

  @Test
  def testNoOpActionResponseWithThrottledChannelWhereThrottlingAlreadyDone(): Unit = {
    val socket = connect()
    val serializedBytes = producerRequestBytes()
    // SendAction with throttling in progress
    val request = throttledChannelTestSetUp(socket, serializedBytes, true, false)

    // Since throttling is already done, the channel can be unmuted.
    TestUtils.waitUntilTrue(() => openOrClosingChannel(request).exists(c => c.muteState() == ChannelMuteState.NOT_MUTED), "fail")
    // Channel is now unmuted.
    assertFalse(openOrClosingChannel(request).exists(c => c.isMuted()))
  }

  @Test
  def testSocketsCloseOnShutdown(): Unit = {
    // open a connection
    val plainSocket = connect()
    plainSocket.setTcpNoDelay(true)
    val bytes = new Array[Byte](40)
    // send a request first to make sure the connection has been picked up by the socket server
    sendRequest(plainSocket, bytes, Some(0))
    processRequest(server.dataPlaneRequestChannel)
    // the following sleep is necessary to reliably detect the connection close when we send data below
    Thread.sleep(200L)
    // make sure the sockets are open
    server.dataPlaneAcceptors.asScala.values.foreach(acceptor => assertFalse(acceptor.serverChannel.socket.isClosed))
    // then shutdown the server
    shutdownServerAndMetrics(server)

    val largeChunkOfBytes = new Array[Byte](1000000)
    // doing a subsequent send should throw an exception as the connection should be closed.
    // send a large chunk of bytes to trigger a socket flush
    try {
      sendRequest(plainSocket, largeChunkOfBytes, Some(0))
      fail("expected exception when writing to closed plain socket")
    } catch {
      case _: IOException => // expected
    }
  }

  @Test
  def testMaxConnectionsPerIp(): Unit = {
    // make the maximum allowable number of connections
    val conns = (0 until server.config.maxConnectionsPerIp).map(_ => connect())
    // now try one more (should fail)
    val conn = connect()
    conn.setSoTimeout(3000)
    assertEquals(-1, conn.getInputStream.read())
    conn.close()

    // it should succeed after closing one connection
    val address = conns.head.getInetAddress
    conns.head.close()
    TestUtils.waitUntilTrue(() => server.connectionCount(address) < conns.length,
      "Failed to decrement connection count after close")
    val conn2 = connect()
    val serializedBytes = producerRequestBytes()
    sendRequest(conn2, serializedBytes)
    val request = server.dataPlaneRequestChannel.receiveRequest(2000)
    assertNotNull(request)
  }

  @Test
  def testZeroMaxConnectionsPerIp(): Unit = {
    val newProps = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 0)
    newProps.setProperty(KafkaConfig.MaxConnectionsPerIpProp, "0")
    newProps.setProperty(KafkaConfig.MaxConnectionsPerIpOverridesProp, "%s:%s".format("127.0.0.1", "5"))
    val server = new SocketServer(KafkaConfig.fromProps(newProps), new Metrics(), Time.SYSTEM, credentialProvider)
    try {
      server.startup()
      // make the maximum allowable number of connections
      val conns = (0 until 5).map(_ => connect(server))
      // now try one more (should fail)
      val conn = connect(server)
      conn.setSoTimeout(3000)
      assertEquals(-1, conn.getInputStream.read())
      conn.close()

      // it should succeed after closing one connection
      val address = conns.head.getInetAddress
      conns.head.close()
      TestUtils.waitUntilTrue(() => server.connectionCount(address) < conns.length,
        "Failed to decrement connection count after close")
      val conn2 = connect(server)
      val serializedBytes = producerRequestBytes()
      sendRequest(conn2, serializedBytes)
      val request = server.dataPlaneRequestChannel.receiveRequest(2000)
      assertNotNull(request)

      // now try to connect from the external facing interface, which should fail
      val conn3 = connect(s = server, localAddr = InetAddress.getLocalHost)
      conn3.setSoTimeout(3000)
      assertEquals(-1, conn3.getInputStream.read())
      conn3.close()
    } finally {
      shutdownServerAndMetrics(server)
    }
  }

  @Test
  def testMaxConnectionsPerIpOverrides(): Unit = {
    val overrideNum = server.config.maxConnectionsPerIp + 1
    val overrideProps = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 0)
    overrideProps.put(KafkaConfig.MaxConnectionsPerIpOverridesProp, s"localhost:$overrideNum")
    val serverMetrics = new Metrics()
    val overrideServer = new SocketServer(KafkaConfig.fromProps(overrideProps), serverMetrics, Time.SYSTEM, credentialProvider)
    try {
      overrideServer.startup()
      // make the maximum allowable number of connections
      val conns = (0 until overrideNum).map(_ => connect(overrideServer))

      // it should succeed
      val serializedBytes = producerRequestBytes()
      sendRequest(conns.last, serializedBytes)
      val request = overrideServer.dataPlaneRequestChannel.receiveRequest(2000)
      assertNotNull(request)

      // now try one more (should fail)
      val conn = connect(overrideServer)
      conn.setSoTimeout(3000)
      assertEquals(-1, conn.getInputStream.read())
    } finally {
      shutdownServerAndMetrics(overrideServer)
    }
  }

  @Test
  def testSslSocketServer(): Unit = {
    val serverMetrics = new Metrics
    val overrideServer = new SocketServer(KafkaConfig.fromProps(sslServerProps), serverMetrics, Time.SYSTEM, credentialProvider)
    try {
      overrideServer.startup()
      val sslContext = SSLContext.getInstance(TestSslUtils.DEFAULT_TLS_PROTOCOL_FOR_TESTS)
      sslContext.init(null, Array(TestUtils.trustAllCerts), new java.security.SecureRandom())
      val socketFactory = sslContext.getSocketFactory
      val sslSocket = socketFactory.createSocket("localhost",
        overrideServer.boundPort(ListenerName.forSecurityProtocol(SecurityProtocol.SSL))).asInstanceOf[SSLSocket]
      sslSocket.setNeedClientAuth(false)

      val correlationId = -1
      val clientId = ""
      val ackTimeoutMs = 10000
      val ack = 0: Short
      val emptyRequest = ProduceRequest.Builder.forCurrentMagic(ack, ackTimeoutMs,
        new HashMap[TopicPartition, MemoryRecords]()).build()
      val emptyHeader = new RequestHeader(ApiKeys.PRODUCE, emptyRequest.version, clientId, correlationId)

      val byteBuffer = emptyRequest.serialize(emptyHeader)
      byteBuffer.rewind()
      val serializedBytes = new Array[Byte](byteBuffer.remaining)
      byteBuffer.get(serializedBytes)

      sendRequest(sslSocket, serializedBytes)
      processRequest(overrideServer.dataPlaneRequestChannel)
      assertEquals(serializedBytes.toSeq, receiveResponse(sslSocket).toSeq)
      sslSocket.close()
    } finally {
      shutdownServerAndMetrics(overrideServer)
    }
  }

  @Test
  def testSaslReauthenticationFailureWithKip152SaslAuthenticate(): Unit = {
    checkSaslReauthenticationFailure(true)
  }

  @Test
  def testSaslReauthenticationFailureNoKip152SaslAuthenticate(): Unit = {
    checkSaslReauthenticationFailure(false)
  }

  def checkSaslReauthenticationFailure(leverageKip152SaslAuthenticateRequest : Boolean): Unit = {
    shutdownServerAndMetrics(server) // we will use our own instance because we require custom configs
    val username = "admin"
    val password = "admin-secret"
    val reauthMs = 1500
    val brokerProps = new Properties
    brokerProps.setProperty("listeners", "SASL_PLAINTEXT://localhost:0")
    brokerProps.setProperty("security.inter.broker.protocol", "SASL_PLAINTEXT")
    brokerProps.setProperty("listener.name.sasl_plaintext.plain.sasl.jaas.config",
      "org.apache.kafka.common.security.plain.PlainLoginModule required " +
        "username=\"%s\" password=\"%s\" user_%s=\"%s\";".format(username, password, username, password))
    brokerProps.setProperty("sasl.mechanism.inter.broker.protocol", "PLAIN")
    brokerProps.setProperty("listener.name.sasl_plaintext.sasl.enabled.mechanisms", "PLAIN")
    brokerProps.setProperty("num.network.threads", "1")
    brokerProps.setProperty("connections.max.reauth.ms", reauthMs.toString)
    val overrideProps = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect,
      saslProperties = Some(brokerProps), enableSaslPlaintext = true)
    val time = new MockTime()
    val overrideServer = new TestableSocketServer(KafkaConfig.fromProps(overrideProps), time = time)
    try {
      overrideServer.startup()
      val socket = connect(overrideServer, ListenerName.forSecurityProtocol(SecurityProtocol.SASL_PLAINTEXT))

      val correlationId = -1
      val clientId = ""
      // send a SASL handshake request
      val version : Short = if (leverageKip152SaslAuthenticateRequest) ApiKeys.SASL_HANDSHAKE.latestVersion else 0
      val saslHandshakeRequest = new SaslHandshakeRequest.Builder(new SaslHandshakeRequestData().setMechanism("PLAIN"))
        .build(version)
      val saslHandshakeHeader = new RequestHeader(ApiKeys.SASL_HANDSHAKE, saslHandshakeRequest.version, clientId,
        correlationId)
      sendApiRequest(socket, saslHandshakeRequest, saslHandshakeHeader)
      receiveResponse(socket)

      // now send credentials
      val authBytes = "admin\u0000admin\u0000admin-secret".getBytes("UTF-8")
      if (leverageKip152SaslAuthenticateRequest) {
        // send credentials within a SaslAuthenticateRequest
        val saslAuthenticateRequest = new SaslAuthenticateRequest.Builder(new SaslAuthenticateRequestData()
          .setAuthBytes(authBytes)).build()
        val saslAuthenticateHeader = new RequestHeader(ApiKeys.SASL_AUTHENTICATE, saslAuthenticateRequest.version,
          clientId, correlationId)
        sendApiRequest(socket, saslAuthenticateRequest, saslAuthenticateHeader)
      } else {
        // send credentials directly, without a SaslAuthenticateRequest
        sendRequest(socket, authBytes)
      }
      receiveResponse(socket)
      assertEquals(1, overrideServer.testableSelector.channels.size)

      // advance the clock long enough to cause server-side disconnection upon next send...
      time.sleep(reauthMs * 2)
      // ...and now send something to trigger the disconnection
      val ackTimeoutMs = 10000
      val ack = 0: Short
      val emptyRequest = ProduceRequest.Builder.forCurrentMagic(ack, ackTimeoutMs,
        new HashMap[TopicPartition, MemoryRecords]()).build()
      val emptyHeader = new RequestHeader(ApiKeys.PRODUCE, emptyRequest.version, clientId, correlationId)
      sendApiRequest(socket, emptyRequest, emptyHeader)
      // wait a little bit for the server-side disconnection to occur since it happens asynchronously
      try {
        TestUtils.waitUntilTrue(() => overrideServer.testableSelector.channels.isEmpty,
          "Expired connection was not closed", 1000, 100)
      } finally {
        socket.close()
      }
    } finally {
      shutdownServerAndMetrics(overrideServer)
    }
  }

  @Test
  def testSessionPrincipal(): Unit = {
    val socket = connect()
    val bytes = new Array[Byte](40)
    sendRequest(socket, bytes, Some(0))
    assertEquals(KafkaPrincipal.ANONYMOUS, receiveRequest(server.dataPlaneRequestChannel).session.principal)
  }

  /* Test that we update request metrics if the client closes the connection while the broker response is in flight. */
  @Test
  def testClientDisconnectionUpdatesRequestMetrics(): Unit = {
    // The way we detect a connection close from the client depends on the response size. If it's small, an
    // IOException ("Connection reset by peer") is thrown when the Selector reads from the socket. If
    // it's large, an IOException ("Broken pipe") is thrown when the Selector writes to the socket. We test
    // both paths to ensure they are handled correctly.
    checkClientDisconnectionUpdatesRequestMetrics(0)
    checkClientDisconnectionUpdatesRequestMetrics(550000)
  }

  private def checkClientDisconnectionUpdatesRequestMetrics(responseBufferSize: Int): Unit = {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 0)
    val serverMetrics = new Metrics
    var conn: Socket = null
    val overrideServer = new SocketServer(KafkaConfig.fromProps(props), serverMetrics, Time.SYSTEM, credentialProvider) {
      override def newProcessor(id: Int, requestChannel: RequestChannel, connectionQuotas: ConnectionQuotas, listenerName: ListenerName,
                                protocol: SecurityProtocol, memoryPool: MemoryPool): Processor = {
        new Processor(id, time, config.socketRequestMaxBytes, dataPlaneRequestChannel, connectionQuotas,
          config.connectionsMaxIdleMs, config.failedAuthenticationDelayMs, listenerName, protocol, config, metrics,
          credentialProvider, MemoryPool.NONE, new LogContext()) {
          override protected[network] def sendResponse(response: RequestChannel.Response, responseSend: Send): Unit = {
            conn.close()
            super.sendResponse(response, responseSend)
          }
        }
      }
    }
    try {
      overrideServer.startup()
      conn = connect(overrideServer)
      val serializedBytes = producerRequestBytes()
      sendRequest(conn, serializedBytes)

      val channel = overrideServer.dataPlaneRequestChannel
      val request = receiveRequest(channel)

      val requestMetrics = channel.metrics(request.header.apiKey.name)
      def totalTimeHistCount(): Long = requestMetrics.totalTimeHist.count
      val send = new NetworkSend(request.context.connectionId, ByteBuffer.allocate(responseBufferSize))
      channel.sendResponse(new RequestChannel.SendResponse(request, send, Some("someResponse"), None))

      val expectedTotalTimeCount = totalTimeHistCount() + 1
      TestUtils.waitUntilTrue(() => totalTimeHistCount() == expectedTotalTimeCount,
        s"request metrics not updated, expected: $expectedTotalTimeCount, actual: ${totalTimeHistCount()}")

    } finally {
      shutdownServerAndMetrics(overrideServer)
    }
  }

  @Test
  def testClientDisconnectionWithOutstandingReceivesProcessedUntilFailedSend() {
    val serverMetrics = new Metrics
    @volatile var selector: TestableSelector = null
    val overrideServer = new SocketServer(KafkaConfig.fromProps(props), serverMetrics, Time.SYSTEM, credentialProvider) {
      override def newProcessor(id: Int, requestChannel: RequestChannel, connectionQuotas: ConnectionQuotas, listenerName: ListenerName,
                                protocol: SecurityProtocol, memoryPool: MemoryPool): Processor = {
        new Processor(id, time, config.socketRequestMaxBytes, dataPlaneRequestChannel, connectionQuotas,
          config.connectionsMaxIdleMs, config.failedAuthenticationDelayMs, listenerName, protocol, config, metrics,
          credentialProvider, memoryPool, new LogContext()) {
          override protected[network] def createSelector(channelBuilder: ChannelBuilder): Selector = {
           val testableSelector = new TestableSelector(config, channelBuilder, time, metrics)
           selector = testableSelector
           testableSelector
        }
        }
      }
    }

    try {
      overrideServer.startup()

      // Create a channel, send some requests and close socket. Receive one pending request after socket was closed.
      val request = closeSocketWithPendingRequest(overrideServer, () => connect(overrideServer))

      // Complete request with socket exception so that the channel is closed
      processRequest(overrideServer.dataPlaneRequestChannel, request)
      TestUtils.waitUntilTrue(() => openOrClosingChannel(request, overrideServer).isEmpty, "Channel not closed after failed send")
      assertTrue("Unexpected completed send", selector.completedSends.isEmpty)
    } finally {
      overrideServer.shutdown()
      serverMetrics.close()
    }
  }

  /*
   * Test that we update request metrics if the channel has been removed from the selector when the broker calls
   * `selector.send` (selector closes old connections, for example).
   */
  @Test
  def testBrokerSendAfterChannelClosedUpdatesRequestMetrics(): Unit = {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 0)
    props.setProperty(KafkaConfig.ConnectionsMaxIdleMsProp, "110")
    val serverMetrics = new Metrics
    var conn: Socket = null
    val overrideServer = new SocketServer(KafkaConfig.fromProps(props), serverMetrics, Time.SYSTEM, credentialProvider)
    try {
      overrideServer.startup()
      conn = connect(overrideServer)
      val serializedBytes = producerRequestBytes()
      sendRequest(conn, serializedBytes)
      val channel = overrideServer.dataPlaneRequestChannel
      val request = receiveRequest(channel)

      TestUtils.waitUntilTrue(() => overrideServer.dataPlaneProcessor(request.processor).channel(request.context.connectionId).isEmpty,
        s"Idle connection `${request.context.connectionId}` was not closed by selector")

      val requestMetrics = channel.metrics(request.header.apiKey.name)
      def totalTimeHistCount(): Long = requestMetrics.totalTimeHist.count
      val expectedTotalTimeCount = totalTimeHistCount() + 1

      processRequest(channel, request)

      TestUtils.waitUntilTrue(() => totalTimeHistCount() == expectedTotalTimeCount,
        s"request metrics not updated, expected: $expectedTotalTimeCount, actual: ${totalTimeHistCount()}")

    } finally {
      shutdownServerAndMetrics(overrideServer)
    }
  }

  @Test
  def testRequestMetricsAfterStop(): Unit = {
    server.stopProcessingRequests()
    val version = ApiKeys.PRODUCE.latestVersion
    val version2 = (version - 1).toShort
    for (_ <- 0 to 1) server.dataPlaneRequestChannel.metrics(ApiKeys.PRODUCE.name).requestRate(version).mark()
    server.dataPlaneRequestChannel.metrics(ApiKeys.PRODUCE.name).requestRate(version2).mark()
    assertEquals(2, server.dataPlaneRequestChannel.metrics(ApiKeys.PRODUCE.name).requestRate(version).count())
    server.dataPlaneRequestChannel.updateErrorMetrics(ApiKeys.PRODUCE, Map(Errors.NONE -> 1))
    val nonZeroMeters = Map(s"kafka.network:type=RequestMetrics,name=RequestsPerSec,request=Produce,version=$version" -> 2,
        s"kafka.network:type=RequestMetrics,name=RequestsPerSec,request=Produce,version=$version2" -> 1,
        "kafka.network:type=RequestMetrics,name=ErrorsPerSec,request=Produce,error=NONE" -> 1)

    def requestMetricMeters = YammerMetrics
      .defaultRegistry
      .allMetrics.asScala
      .collect { case (k, metric: Meter) if k.getType == "RequestMetrics" => (k.toString, metric.count) }

    assertEquals(nonZeroMeters, requestMetricMeters.filter { case (_, value) => value != 0 })
    server.shutdown()
    assertEquals(Map.empty, requestMetricMeters)
  }

  @Test
  def testMetricCollectionAfterShutdown(): Unit = {
    server.shutdown()

    val nonZeroMetricNamesAndValues = YammerMetrics
      .defaultRegistry
      .allMetrics.asScala
      .filter { case (k, _) => k.getName.endsWith("IdlePercent") || k.getName.endsWith("NetworkProcessorAvgIdlePercent") }
      .collect { case (k, metric: Gauge[_]) => (k, metric.value().asInstanceOf[Double]) }
      .filter { case (_, value) => value != 0.0 && !value.equals(Double.NaN) }

    assertEquals(Map.empty, nonZeroMetricNamesAndValues)
  }

  @Test
  def testProcessorMetricsTags(): Unit = {
    val kafkaMetricNames = metrics.metrics.keySet.asScala.filter(_.tags.asScala.get("listener").nonEmpty)
    assertFalse(kafkaMetricNames.isEmpty)

    val expectedListeners = Set("PLAINTEXT")
    kafkaMetricNames.foreach { kafkaMetricName =>
      assertTrue(expectedListeners.contains(kafkaMetricName.tags.get("listener")))
    }

    // legacy metrics not tagged
    val yammerMetricsNames = YammerMetrics.defaultRegistry.allMetrics.asScala
      .filterKeys(_.getType.equals("Processor"))
      .collect { case (k, _: Gauge[_]) => k }
    assertFalse(yammerMetricsNames.isEmpty)

    yammerMetricsNames.foreach { yammerMetricName =>
      assertFalse(yammerMetricName.getMBeanName.contains("listener="))
    }
  }

  /**
   * Tests exception handling in [[Processor.configureNewConnections]]. Exception is
   * injected into [[Selector.register]] which is used to register each new connection.
   * Test creates two connections in a single iteration by waking up the selector only
   * when two connections are ready.
   * Verifies that
   * - first failed connection is closed
   * - second connection is processed successfully after the first fails with an exception
   * - processor is healthy after the exception
   */
  @Test
  def configureNewConnectionException(): Unit = {
    withTestableServer (testWithServer = { testableServer =>
      val testableSelector = testableServer.testableSelector

      testableSelector.updateMinWakeup(2)
      testableSelector.addFailure(SelectorOperation.Register)
      val sockets = (1 to 2).map(_ => connect(testableServer))
      testableSelector.waitForOperations(SelectorOperation.Register, 2)
      TestUtils.waitUntilTrue(() => testableServer.connectionCount(localAddress) == 1, "Failed channel not removed")

      assertProcessorHealthy(testableServer, testableSelector.notFailed(sockets))
    })
  }

  /**
   * Tests exception handling in [[Processor.processNewResponses]]. Exception is
   * injected into [[Selector.send]] which is used to send the new response.
   * Test creates two responses in a single iteration by waking up the selector only
   * when two responses are ready.
   * Verifies that
   * - first failed channel is closed
   * - second response is processed successfully after the first fails with an exception
   * - processor is healthy after the exception
   */
  @Test
  def processNewResponseException(): Unit = {
    withTestableServer (testWithServer = { testableServer =>
      val testableSelector = testableServer.testableSelector
      testableSelector.updateMinWakeup(2)

      val sockets = (1 to 2).map(_ => connect(testableServer))
      sockets.foreach(sendRequest(_, producerRequestBytes()))

      testableServer.testableSelector.addFailure(SelectorOperation.Send)
      sockets.foreach(_ => processRequest(testableServer.dataPlaneRequestChannel))
      testableSelector.waitForOperations(SelectorOperation.Send, 2)
      testableServer.waitForChannelClose(testableSelector.allFailedChannels.head, locallyClosed = true)

      assertProcessorHealthy(testableServer, testableSelector.notFailed(sockets))
    })
  }

  /**
   * Tests exception handling in [[Processor.processNewResponses]] when [[Selector.send]]
   * fails with `CancelledKeyException`, which is handled by the selector using a different
   * code path. Test scenario is similar to [[SocketServerTest.processNewResponseException]].
   */
  @Test
  def sendCancelledKeyException(): Unit = {
    withTestableServer (testWithServer = { testableServer =>
      val testableSelector = testableServer.testableSelector
      testableSelector.updateMinWakeup(2)

      val sockets = (1 to 2).map(_ => connect(testableServer))
      sockets.foreach(sendRequest(_, producerRequestBytes()))
      val requestChannel = testableServer.dataPlaneRequestChannel

      val requests = sockets.map(_ => receiveRequest(requestChannel))
      val failedConnectionId = requests(0).context.connectionId
      // `KafkaChannel.disconnect()` cancels the selection key, triggering CancelledKeyException during send
      testableSelector.channel(failedConnectionId).disconnect()
      requests.foreach(processRequest(requestChannel, _))
      testableSelector.waitForOperations(SelectorOperation.Send, 2)
      testableServer.waitForChannelClose(failedConnectionId, locallyClosed = false)

      val successfulSocket = if (isSocketConnectionId(failedConnectionId, sockets(0))) sockets(1) else sockets(0)
      assertProcessorHealthy(testableServer, Seq(successfulSocket))
    })
  }

  /**
   * Tests channel send failure handling when send failure is triggered by [[Selector.send]]
   * to a channel whose peer has closed its connection.
   */
  @Test
  def remoteCloseSendFailure(): Unit = {
    verifySendFailureAfterRemoteClose(makeClosing = false)
  }

  /**
   * Tests channel send failure handling when send failure is triggered by [[Selector.send]]
   * to a channel whose peer has closed its connection and the channel is in `closingChannels`.
   */
  @Test
  def closingChannelSendFailure(): Unit = {
    verifySendFailureAfterRemoteClose(makeClosing = true)
  }

  private def verifySendFailureAfterRemoteClose(makeClosing: Boolean): Unit = {
    props ++= sslServerProps
    withTestableServer (testWithServer = { testableServer =>
      val testableSelector = testableServer.testableSelector

      val serializedBytes = producerRequestBytes()
      val request = makeChannelWithBufferedRequestsAndCloseRemote(testableServer, testableSelector, makeClosing)
      val otherSocket = sslConnect(testableServer)
      sendRequest(otherSocket, serializedBytes)

      processRequest(testableServer.dataPlaneRequestChannel, request)
      processRequest(testableServer.dataPlaneRequestChannel) // Also process request from other socket
      testableSelector.waitForOperations(SelectorOperation.Send, 2)
      testableServer.waitForChannelClose(request.context.connectionId, locallyClosed = false)

      assertProcessorHealthy(testableServer, Seq(otherSocket))
    })
  }

  /**
   * Verifies that all pending buffered receives are processed even if remote connection is closed.
   * The channel must be closed after pending receives are processed.
   */
  @Test
  def remoteCloseWithBufferedReceives(): Unit = {
    verifyRemoteCloseWithBufferedReceives(numComplete = 3, hasIncomplete = false)
  }

  /**
   * Verifies that channel is closed when remote client closes its connection if there is no
   * buffered receive.
   */
  @Test
  def remoteCloseWithoutBufferedReceives(): Unit = {
    verifyRemoteCloseWithBufferedReceives(numComplete = 0, hasIncomplete = false)
  }

  /**
   * Verifies that channel is closed when remote client closes its connection if there is a pending
   * receive that is incomplete.
   */
  @Test
  def remoteCloseWithIncompleteBufferedReceive(): Unit = {
    verifyRemoteCloseWithBufferedReceives(numComplete = 0, hasIncomplete = true)
  }

  /**
   * Verifies that all pending buffered receives are processed even if remote connection is closed.
   * The channel must be closed after complete receives are processed, even if there is an incomplete
   * receive remaining in the buffers.
   */
  @Test
  def remoteCloseWithCompleteAndIncompleteBufferedReceives(): Unit = {
    verifyRemoteCloseWithBufferedReceives(numComplete = 3, hasIncomplete = true)
  }

  /**
   * Verifies that pending buffered receives are processed when remote connection is closed
   * until a response send fails.
   */
  @Test
  def remoteCloseWithBufferedReceivesFailedSend(): Unit = {
    verifyRemoteCloseWithBufferedReceives(numComplete = 3, hasIncomplete = false, responseRequiredIndex = 1)
  }

  /**
   * Verifies that all pending buffered receives are processed for channel in closing state.
   * The channel must be closed after pending receives are processed.
   */
  @Test
  def closingChannelWithBufferedReceives(): Unit = {
    verifyRemoteCloseWithBufferedReceives(numComplete = 3, hasIncomplete = false, makeClosing = true)
  }

  /**
   * Verifies that all pending buffered receives are processed for channel in closing state.
   * The channel must be closed after complete receives are processed, even if there is an incomplete
   * receive remaining in the buffers.
   */
  @Test
  def closingChannelWithCompleteAndIncompleteBufferedReceives(): Unit = {
    verifyRemoteCloseWithBufferedReceives(numComplete = 3, hasIncomplete = true, makeClosing = false)
  }

  /**
   * Verifies that pending buffered receives are processed for a channel in closing state
   * until a response send fails.
   */
  @Test
  def closingChannelWithBufferedReceivesFailedSend(): Unit = {
    verifyRemoteCloseWithBufferedReceives(numComplete = 3, hasIncomplete = false, responseRequiredIndex = 1, makeClosing = false)
  }

  private def verifyRemoteCloseWithBufferedReceives(numComplete: Int,
                                                    hasIncomplete: Boolean,
                                                    responseRequiredIndex: Int = -1,
                                                    makeClosing: Boolean = false): Unit = {
    props ++= sslServerProps

    def truncateBufferedRequest(channel: KafkaChannel): Unit = {
      val transportLayer: SslTransportLayer = JTestUtils.fieldValue(channel, classOf[KafkaChannel], "transportLayer")
      val netReadBuffer: ByteBuffer = JTestUtils.fieldValue(transportLayer, classOf[SslTransportLayer], "netReadBuffer")
      val appReadBuffer: ByteBuffer = JTestUtils.fieldValue(transportLayer, classOf[SslTransportLayer], "appReadBuffer")
      if (appReadBuffer.position() > 4) {
        appReadBuffer.position(4)
        netReadBuffer.position(0)
      } else {
        netReadBuffer.position(20)
      }
    }
    withTestableServer (testWithServer = { testableServer =>
      val testableSelector = testableServer.testableSelector

      val proxyServer = new ProxyServer(testableServer)
      try {
        val numBufferedRequests = numComplete + (if (hasIncomplete) 1 else 0)
        val (socket, request1) = makeSocketWithBufferedRequests(testableServer, testableSelector, proxyServer, numBufferedRequests)
        val channel = openChannel(request1, testableServer).getOrElse(throw new IllegalStateException("Channel closed too early"))

        socket.close()
        proxyServer.serverConnSocket.close()
        TestUtils.waitUntilTrue(() => proxyServer.clientConnSocket.isClosed, "Client socket not closed")
        if (makeClosing)
          testableSelector.pendingClosingChannels.add(channel)
        if (numComplete == 0 && hasIncomplete)
          truncateBufferedRequest(channel)

        processRequestNoOpResponse(testableServer.dataPlaneRequestChannel, request1)
        assertSame(channel, openOrClosingChannel(request1, testableServer).getOrElse(throw new IllegalStateException("Channel closed too early")))

        val numRequests = if (responseRequiredIndex >= 0) responseRequiredIndex + 1 else numComplete
        (0 until numRequests).foreach { i =>
          val request = receiveRequest(testableServer.dataPlaneRequestChannel)
          if (i == numComplete - 1 && hasIncomplete)
            truncateBufferedRequest(channel)
          if (responseRequiredIndex == i)
            processRequest(testableServer.dataPlaneRequestChannel, request)
          else
            processRequestNoOpResponse(testableServer.dataPlaneRequestChannel, request)
        }
        testableServer.waitForChannelClose(channel.id, locallyClosed = false)

        val anotherSocket = sslConnect(testableServer)
        assertProcessorHealthy(testableServer, Seq(anotherSocket))
      } finally {
        proxyServer.close()
      }
    })
  }

  /**
   * Tests idle channel expiry for SSL channels with buffered data. Muted channels are expired
   * immediately even if there is pending data to be processed. This is consistent with PLAINTEXT where
   * we expire muted channels even if there is data available on the socket. This scenario occurs if broker
   * takes longer than idle timeout to process a client request. In this case, typically client would have
   * expired its connection and would potentially reconnect to retry the request, so immediate expiry enables
   * the old connection and its associated resources to be freed sooner.
   */
  @Test
  def idleExpiryWithBufferedReceives(): Unit = {
    val idleTimeMs = 60000
    val time = new MockTime()
    props.put(KafkaConfig.ConnectionsMaxIdleMsProp, idleTimeMs.toString)
    props ++= sslServerProps
    val testableServer = new TestableSocketServer(time = time)
    testableServer.startup()
    val proxyServer = new ProxyServer(testableServer)
    try {
      val testableSelector = testableServer.testableSelector
      testableSelector.updateMinWakeup(2)

      val (socket, request) = makeSocketWithBufferedRequests(testableServer, testableSelector, proxyServer)
      time.sleep(idleTimeMs + 1)
      testableServer.waitForChannelClose(request.context.connectionId, locallyClosed = false)

      val otherSocket = sslConnect(testableServer)
      assertProcessorHealthy(testableServer, Seq(otherSocket))

      socket.close()
    } finally {
      proxyServer.close()
      shutdownServerAndMetrics(testableServer)
    }
  }

  /**
   * Tests exception handling in [[Processor.processCompletedReceives]]. Exception is
   * injected into [[Selector.mute]] which is used to mute the channel when a receive is complete.
   * Test creates two receives in a single iteration by caching completed receives until two receives
   * are complete.
   * Verifies that
   * - first failed channel is closed
   * - second receive is processed successfully after the first fails with an exception
   * - processor is healthy after the exception
   */
  @Test
  def processCompletedReceiveException(): Unit = {
    withTestableServer (testWithServer = { testableServer =>
      val sockets = (1 to 2).map(_ => connect(testableServer))
      val testableSelector = testableServer.testableSelector
      val requestChannel = testableServer.dataPlaneRequestChannel

      testableSelector.cachedCompletedReceives.minPerPoll = 2
      testableSelector.addFailure(SelectorOperation.Mute)
      sockets.foreach(sendRequest(_, producerRequestBytes()))
      val requests = sockets.map(_ => receiveRequest(requestChannel))
      testableSelector.waitForOperations(SelectorOperation.Mute, 2)
      testableServer.waitForChannelClose(testableSelector.allFailedChannels.head, locallyClosed = true)
      requests.foreach(processRequest(requestChannel, _))

      assertProcessorHealthy(testableServer, testableSelector.notFailed(sockets))
    })
  }

  /**
   * Tests exception handling in [[Processor.processCompletedSends]]. Exception is
   * injected into [[Selector.unmute]] which is used to unmute the channel after send is complete.
   * Test creates two completed sends in a single iteration by caching completed sends until two
   * sends are complete.
   * Verifies that
   * - first failed channel is closed
   * - second send is processed successfully after the first fails with an exception
   * - processor is healthy after the exception
   */
  @Test
  def processCompletedSendException(): Unit = {
    withTestableServer (testWithServer = { testableServer =>
      val testableSelector = testableServer.testableSelector
      val sockets = (1 to 2).map(_ => connect(testableServer))
      val requests = sockets.map(sendAndReceiveRequest(_, testableServer))

      testableSelector.addFailure(SelectorOperation.Unmute)
      requests.foreach(processRequest(testableServer.dataPlaneRequestChannel, _))
      testableSelector.waitForOperations(SelectorOperation.Unmute, 2)
      testableServer.waitForChannelClose(testableSelector.allFailedChannels.head, locallyClosed = true)

      assertProcessorHealthy(testableServer, testableSelector.notFailed(sockets))
    })
  }

  /**
   * Tests exception handling in [[Processor.processDisconnected]]. An invalid connectionId
   * is inserted to the disconnected list just before the actual valid one.
   * Verifies that
   * - first invalid connectionId is ignored
   * - second disconnected channel is processed successfully after the first fails with an exception
   * - processor is healthy after the exception
   */
  @Test
  def processDisconnectedException(): Unit = {
    withTestableServer (testWithServer = { testableServer =>
      val (socket, connectionId) = connectAndProcessRequest(testableServer)
      val testableSelector = testableServer.testableSelector

      // Add an invalid connectionId to `Selector.disconnected` list before the actual disconnected channel
      // and check that the actual id is processed and the invalid one ignored.
      testableSelector.cachedDisconnected.minPerPoll = 2
      testableSelector.cachedDisconnected.deferredValues += "notAValidConnectionId" -> ChannelState.EXPIRED
      socket.close()
      testableSelector.operationCounts.clear()
      testableSelector.waitForOperations(SelectorOperation.Poll, 1)
      testableServer.waitForChannelClose(connectionId, locallyClosed = false)

      assertProcessorHealthy(testableServer)
    })
  }

  /**
   * Tests that `Processor` continues to function correctly after a failed [[Selector.poll]].
   */
  @Test
  def pollException(): Unit = {
    withTestableServer (testWithServer = { testableServer =>
      val (socket, _) = connectAndProcessRequest(testableServer)
      val testableSelector = testableServer.testableSelector

      testableSelector.addFailure(SelectorOperation.Poll)
      testableSelector.operationCounts.clear()
      testableSelector.waitForOperations(SelectorOperation.Poll, 2)

      assertProcessorHealthy(testableServer, Seq(socket))
    })
  }

  /**
   * Tests handling of `ControlThrowable`. Verifies that the selector is closed.
   */
  @Test
  def controlThrowable(): Unit = {
    withTestableServer (testWithServer = { testableServer =>
      connectAndProcessRequest(testableServer)
      val testableSelector = testableServer.testableSelector

      testableSelector.operationCounts.clear()
      testableSelector.addFailure(SelectorOperation.Poll,
          Some(new ControlThrowable() {}))
      testableSelector.waitForOperations(SelectorOperation.Poll, 1)

      testableSelector.waitForOperations(SelectorOperation.CloseSelector, 1)
    })
  }

  @Test
  def testConnectionRateLimit(): Unit = {
    shutdownServerAndMetrics(server)
    val numConnections = 5
    props.put("max.connections.per.ip", numConnections.toString)
    val testableServer = new TestableSocketServer(KafkaConfig.fromProps(props), connectionQueueSize = 1)
    testableServer.startup()
    val testableSelector = testableServer.testableSelector
    val errors = new mutable.HashSet[String]

    def acceptorStackTraces: scala.collection.Map[Thread, String] = {
      Thread.getAllStackTraces.asScala.collect {
        case (thread, stacktraceElement) if thread.getName.contains("kafka-socket-acceptor") =>
          thread -> stacktraceElement.mkString("\n")
      }
    }

    def acceptorBlocked: Boolean = {
      val stackTraces = acceptorStackTraces
      if (stackTraces.isEmpty)
        errors.add(s"Acceptor thread not found, threads=${Thread.getAllStackTraces.keySet}")
      stackTraces.exists { case (thread, stackTrace) =>
          thread.getState == Thread.State.WAITING && stackTrace.contains("ArrayBlockingQueue")
      }
    }

    def registeredConnectionCount: Int = testableSelector.operationCounts.getOrElse(SelectorOperation.Register, 0)

    try {
      // Block selector until Acceptor is blocked while connections are pending
      testableSelector.pollCallback = () => {
        try {
          TestUtils.waitUntilTrue(() => errors.nonEmpty || registeredConnectionCount >= numConnections - 1 || acceptorBlocked,
            "Acceptor not blocked", waitTimeMs = 10000)
        } catch {
          case _: Throwable => errors.add(s"Acceptor not blocked: $acceptorStackTraces")
        }
      }
      testableSelector.operationCounts.clear()
      val sockets = (1 to numConnections).map(_ => connect(testableServer))
      TestUtils.waitUntilTrue(() => errors.nonEmpty || registeredConnectionCount == numConnections,
        "Connections not registered", waitTimeMs = 15000)
      assertEquals(Set.empty, errors)
      testableSelector.waitForOperations(SelectorOperation.Register, numConnections)

      // In each iteration, SocketServer processes at most connectionQueueSize (1 in this test)
      // new connections and then does poll() to process data from existing connections. So for
      // 5 connections, we expect 5 iterations. Since we stop when the 5th connection is processed,
      // we can safely check that there were atleast 4 polls prior to the 5th connection.
      val pollCount = testableSelector.operationCounts(SelectorOperation.Poll)
      assertTrue(s"Connections created too quickly: $pollCount", pollCount >= numConnections - 1)
      verifyAcceptorBlockedPercent("PLAINTEXT", expectBlocked = true)

      assertProcessorHealthy(testableServer, sockets)
    } finally {
      shutdownServerAndMetrics(testableServer)
    }
  }

  private def sslServerProps: Properties = {
    val trustStoreFile = File.createTempFile("truststore", ".jks")
    val sslProps = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, interBrokerSecurityProtocol = Some(SecurityProtocol.SSL),
      trustStoreFile = Some(trustStoreFile))
    sslProps.put(KafkaConfig.ListenersProp, "SSL://localhost:0")
    sslProps
  }

  private def withTestableServer(config : KafkaConfig = KafkaConfig.fromProps(props),
                                 testWithServer: TestableSocketServer => Unit): Unit = {
    val testableServer = new TestableSocketServer(config)
    testableServer.startup()
    try {
        testWithServer(testableServer)
    } finally {
      shutdownServerAndMetrics(testableServer)
    }
  }

  def sendAndReceiveControllerRequest(socket: Socket, server: SocketServer): RequestChannel.Request = {
    sendRequest(socket, producerRequestBytes())
    receiveRequest(server.controlPlaneRequestChannelOpt.get)
  }

  private def assertProcessorHealthy(testableServer: TestableSocketServer, healthySockets: Seq[Socket] = Seq.empty): Unit = {
    val selector = testableServer.testableSelector
    selector.reset()
    val requestChannel = testableServer.dataPlaneRequestChannel

    // Check that existing channels behave as expected
    healthySockets.foreach { socket =>
      val request = sendAndReceiveRequest(socket, testableServer)
      processRequest(requestChannel, request)
      socket.close()
    }
    TestUtils.waitUntilTrue(() => testableServer.connectionCount(localAddress) == 0, "Channels not removed")

    // Check new channel behaves as expected
    val (socket, connectionId) = connectAndProcessRequest(testableServer)
    assertArrayEquals(producerRequestBytes(), receiveResponse(socket))
    assertNotNull("Channel should not have been closed", selector.channel(connectionId))
    assertNull("Channel should not be closing", selector.closingChannel(connectionId))
    socket.close()
    TestUtils.waitUntilTrue(() => testableServer.connectionCount(localAddress) == 0, "Channels not removed")
  }

  // Since all sockets use the same local host, it is sufficient to check the local port
  def isSocketConnectionId(connectionId: String, socket: Socket): Boolean =
    connectionId.contains(s":${socket.getLocalPort}-")

  private def verifyAcceptorBlockedPercent(listenerName: String, expectBlocked: Boolean): Unit = {
    val blockedPercentMetricMBeanName = "kafka.network:type=Acceptor,name=AcceptorBlockedPercent,listener=PLAINTEXT"
    val blockedPercentMetrics = YammerMetrics.defaultRegistry.allMetrics.asScala
      .filterKeys(_.getMBeanName == blockedPercentMetricMBeanName).values
    assertEquals(1, blockedPercentMetrics.size)
    val blockedPercentMetric = blockedPercentMetrics.head.asInstanceOf[Meter]
    val blockedPercent = blockedPercentMetric.meanRate
    if (expectBlocked) {
      assertTrue(s"Acceptor blocked percent not recorded: $blockedPercent", blockedPercent > 0.0)
      assertTrue(s"Unexpected blocked percent in acceptor: $blockedPercent", blockedPercent <= 1.0)
    } else {
      assertEquals(0.0, blockedPercent, 0.001)
    }
  }

  class TestableSocketServer(config : KafkaConfig = KafkaConfig.fromProps(props), val connectionQueueSize: Int = 20,
                             override val time: Time = Time.SYSTEM) extends SocketServer(config,
      new Metrics, time, credentialProvider) {

    @volatile var selector: Option[TestableSelector] = None

    override def newProcessor(id: Int, requestChannel: RequestChannel, connectionQuotas: ConnectionQuotas, listenerName: ListenerName,
                                protocol: SecurityProtocol, memoryPool: MemoryPool): Processor = {
      new Processor(id, time, config.socketRequestMaxBytes, requestChannel, connectionQuotas, config.connectionsMaxIdleMs,
        config.failedAuthenticationDelayMs, listenerName, protocol, config, metrics, credentialProvider,
        memoryPool, new LogContext(), connectionQueueSize) {

        override protected[network] def createSelector(channelBuilder: ChannelBuilder): Selector = {
           val testableSelector = new TestableSelector(config, channelBuilder, time, metrics, metricTags.asScala)
           selector = Some(testableSelector)
           testableSelector
        }
      }
    }

    def testableSelector: TestableSelector =
      selector.getOrElse(throw new IllegalStateException("Selector not created"))

    def waitForChannelClose(connectionId: String, locallyClosed: Boolean): Unit = {
      val selector = testableSelector
      if (locallyClosed) {
        TestUtils.waitUntilTrue(() => selector.allLocallyClosedChannels.contains(connectionId),
            s"Channel not closed: $connectionId")
        assertTrue("Unexpected disconnect notification", testableSelector.allDisconnectedChannels.isEmpty)
      } else {
        TestUtils.waitUntilTrue(() => selector.allDisconnectedChannels.contains(connectionId),
            s"Disconnect notification not received: $connectionId")
        assertTrue("Channel closed locally", testableSelector.allLocallyClosedChannels.isEmpty)
      }
      val openCount = selector.allChannels.size - 1 // minus one for the channel just closed above
      TestUtils.waitUntilTrue(() => connectionCount(localAddress) == openCount, "Connection count not decremented")
      TestUtils.waitUntilTrue(() =>
        dataPlaneProcessor(0).inflightResponseCount == 0, "Inflight responses not cleared")
      assertNull("Channel not removed", selector.channel(connectionId))
      assertNull("Closing channel not removed", selector.closingChannel(connectionId))
    }
  }

  sealed trait SelectorOperation
  object SelectorOperation {
    case object Register extends SelectorOperation
    case object Poll extends SelectorOperation
    case object Send extends SelectorOperation
    case object Mute extends SelectorOperation
    case object Unmute extends SelectorOperation
    case object Wakeup extends SelectorOperation
    case object Close extends SelectorOperation
    case object CloseSelector extends SelectorOperation
  }

  class TestableSelector(config: KafkaConfig, channelBuilder: ChannelBuilder, time: Time, metrics: Metrics, metricTags: mutable.Map[String, String] = mutable.Map.empty)
        extends Selector(config.socketRequestMaxBytes, config.connectionsMaxIdleMs, config.failedAuthenticationDelayMs,
            metrics, time, "socket-server", metricTags.asJava, false, true, channelBuilder, MemoryPool.NONE, new LogContext()) {

    val failures = mutable.Map[SelectorOperation, Throwable]()
    val operationCounts = mutable.Map[SelectorOperation, Int]().withDefaultValue(0)
    val allChannels = mutable.Set[String]()
    val allLocallyClosedChannels = mutable.Set[String]()
    val allDisconnectedChannels = mutable.Set[String]()
    val allFailedChannels = mutable.Set[String]()

    // Enable data from `Selector.poll()` to be deferred to a subsequent poll() until
    // the number of elements of that type reaches `minPerPoll`. This enables tests to verify
    // that failed processing doesn't impact subsequent processing within the same iteration.
    class PollData[T] {
      var minPerPoll = 1
      val deferredValues = mutable.Buffer[T]()
      val currentPollValues = mutable.Buffer[T]()
      def update(newValues: mutable.Buffer[T]): Unit = {
        if (currentPollValues.nonEmpty || deferredValues.size + newValues.size >= minPerPoll) {
          if (deferredValues.nonEmpty) {
            currentPollValues ++= deferredValues
            deferredValues.clear()
          }
          currentPollValues ++= newValues
        } else
          deferredValues ++= newValues
      }
      def reset(): Unit = {
        currentPollValues.clear()
      }
    }
    val cachedCompletedReceives = new PollData[NetworkReceive]()
    val cachedCompletedSends = new PollData[Send]()
    val cachedDisconnected = new PollData[(String, ChannelState)]()
    val allCachedPollData = Seq(cachedCompletedReceives, cachedCompletedSends, cachedDisconnected)
    val pendingClosingChannels = new ConcurrentLinkedQueue[KafkaChannel]()
    @volatile var minWakeupCount = 0
    @volatile var pollTimeoutOverride: Option[Long] = None
    @volatile var pollCallback: () => Unit = () => {}

    def addFailure(operation: SelectorOperation, exception: Option[Throwable] = None): Unit = {
      failures += operation ->
        exception.getOrElse(new IllegalStateException(s"Test exception during $operation"))
    }

    private def onOperation(operation: SelectorOperation, connectionId: Option[String], onFailure: => Unit): Unit = {
      operationCounts(operation) += 1
      failures.remove(operation).foreach { e =>
        connectionId.foreach(allFailedChannels.add)
        onFailure
        throw e
      }
    }

    def waitForOperations(operation: SelectorOperation, minExpectedTotal: Int): Unit = {
      TestUtils.waitUntilTrue(() =>
        operationCounts.getOrElse(operation, 0) >= minExpectedTotal, "Operations not performed within timeout")
    }

    def runOp[T](operation: SelectorOperation, connectionId: Option[String],
        onFailure: => Unit = {})(code: => T): T = {
      // If a failure is set on `operation`, throw that exception even if `code` fails
      try code
      finally onOperation(operation, connectionId, onFailure)
    }

    override def register(id: String, socketChannel: SocketChannel): Unit = {
      runOp(SelectorOperation.Register, Some(id), onFailure = close(id)) {
        super.register(id, socketChannel)
      }
    }

    override def send(s: Send): Unit = {
      runOp(SelectorOperation.Send, Some(s.destination)) {
        super.send(s)
      }
    }

    override def poll(timeout: Long): Unit = {
      try {
        pollCallback.apply()
        while (!pendingClosingChannels.isEmpty) {
          makeClosing(pendingClosingChannels.poll())
        }
        allCachedPollData.foreach(_.reset)
        runOp(SelectorOperation.Poll, None) {
          super.poll(pollTimeoutOverride.getOrElse(timeout))
        }
      } finally {
        super.channels.asScala.foreach(allChannels += _.id)
        allDisconnectedChannels ++= super.disconnected.asScala.keys
        cachedCompletedReceives.update(super.completedReceives.asScala.toBuffer)
        cachedCompletedSends.update(super.completedSends.asScala)
        cachedDisconnected.update(super.disconnected.asScala.toBuffer)
      }
    }

    override def mute(id: String): Unit = {
      runOp(SelectorOperation.Mute, Some(id)) {
        super.mute(id)
      }
    }

    override def unmute(id: String): Unit = {
      runOp(SelectorOperation.Unmute, Some(id)) {
        super.unmute(id)
      }
    }

    override def wakeup(): Unit = {
      runOp(SelectorOperation.Wakeup, None) {
        if (minWakeupCount > 0)
          minWakeupCount -= 1
        if (minWakeupCount <= 0)
          super.wakeup()
      }
    }

    override def disconnected: java.util.Map[String, ChannelState] = cachedDisconnected.currentPollValues.toMap.asJava

    override def completedSends: java.util.List[Send] = cachedCompletedSends.currentPollValues.asJava

    override def completedReceives: java.util.List[NetworkReceive] = cachedCompletedReceives.currentPollValues.asJava

    override def close(id: String): Unit = {
      runOp(SelectorOperation.Close, Some(id)) {
        super.close(id)
        allLocallyClosedChannels += id
      }
    }

    override def close(): Unit = {
      runOp(SelectorOperation.CloseSelector, None) {
        super.close()
      }
    }

    def updateMinWakeup(count: Int): Unit = {
      minWakeupCount = count
      // For tests that ignore wakeup to process responses together, increase poll timeout
      // to ensure that poll doesn't complete before the responses are ready
      pollTimeoutOverride = Some(1000L)
      // Wakeup current poll to force new poll timeout to take effect
      super.wakeup()
    }

    def reset(): Unit = {
      failures.clear()
      allCachedPollData.foreach(_.minPerPoll = 1)
    }

    def notFailed(sockets: Seq[Socket]): Seq[Socket] = {
      // Each test generates failure for exactly one failed channel
      assertEquals(1, allFailedChannels.size)
      val failedConnectionId = allFailedChannels.head
      sockets.filterNot(socket => isSocketConnectionId(failedConnectionId, socket))
    }

    private def makeClosing(channel: KafkaChannel): Unit = {
      val channels: util.Map[String, KafkaChannel] = JTestUtils.fieldValue(this, classOf[Selector], "channels")
      val closingChannels: util.Map[String, KafkaChannel] = JTestUtils.fieldValue(this, classOf[Selector], "closingChannels")
      closingChannels.put(channel.id, channel)
      channels.remove(channel.id)
    }
  }

  private class ProxyServer(socketServer: SocketServer) {
    val serverSocket = new ServerSocket(0)
    val localPort = serverSocket.getLocalPort
    var buffer: Option[ByteBuffer] = None
    val serverConnSocket = new Socket("localhost", socketServer.boundPort(ListenerName.forSecurityProtocol(SecurityProtocol.SSL)))
    val executor = Executors.newFixedThreadPool(2)
    var clientConnSocket: Socket = _

    executor.submit(CoreUtils.runnable {
      try {
        clientConnSocket = serverSocket.accept()
        val serverOut = serverConnSocket.getOutputStream
        val clientIn = clientConnSocket.getInputStream
        var b: Int = -1
        while ({b = clientIn.read(); b != -1}) {
          buffer match {
            case Some(buf) =>
              buf.put(b.asInstanceOf[Byte])
            case None =>
              serverOut.write(b)
              serverOut.flush()
          }
        }
      } finally {
        clientConnSocket.close()
      }

    })

    executor.submit(CoreUtils.runnable {
      var b: Int = -1
      val serverIn = serverConnSocket.getInputStream
      while ({b = serverIn.read(); b != -1}) {
        clientConnSocket.getOutputStream.write(b)
      }
    })

    def enableBuffering(buffer: ByteBuffer): Unit = this.buffer = Some(buffer)

    def sendToClient(response: ByteBuffer): Unit = {
      send(clientConnSocket, response)
    }

    def sendToServer(request: ByteBuffer): Unit = {
      send(serverConnSocket, request)
    }

    private def send(socket: Socket, buffer: ByteBuffer): Unit = {
      socket.getOutputStream.write(buffer.array, 0, buffer.position)
      buffer.clear()
    }

    def close(): Unit = {
      serverSocket.close()
      serverConnSocket.close()
      clientConnSocket.close()
      executor.shutdownNow()
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS))
    }

  }
}
