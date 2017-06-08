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

package kafka.utils

import org.slf4j.{LoggerFactory, MarkerFactory}

import scala.util.{Failure, Success, Try}

object Log4jControllerRegistration {
  private val logger = LoggerFactory.getLogger(this.getClass.getName)

  val FatalMarker = MarkerFactory.getMarker("FATAL")

  val instance = Try {
    val log4jController = Class.forName("kafka.utils.Log4jController").asInstanceOf[Class[Object]]
    val instance = log4jController.newInstance()
    CoreUtils.registerMBean(instance, "kafka:type=kafka.Log4jController")
    instance
  }

  instance match {
    case Success(v) => logger.info("Registered kafka:type=kafka.Log4jController MBean")
    case Failure(v) => logger.info("Couldn't register kafka:type=kafka.Log4jController MBean")
  }
}

trait Logging {
  def loggerName = this.getClass.getName
  lazy val logger = LoggerFactory.getLogger(loggerName)

  protected var logIdent: String = null

  Log4jControllerRegistration

  protected def msgWithLogIdent(msg: String) =
    if (logIdent == null) msg else logIdent + msg

  def trace(msg: => String): Unit = {
    if (logger.isTraceEnabled())
      logger.trace(msgWithLogIdent(msg))
  }
  def trace(e: => Throwable): Any = {
    if (logger.isTraceEnabled())
      logger.trace(logIdent,e)
  }
  def trace(msg: => String, e: => Throwable) = {
    if (logger.isTraceEnabled())
      logger.trace(msgWithLogIdent(msg),e)
  }
  def swallowTrace(action: => Unit) {
    CoreUtils.swallow(logger.trace, action)
  }

  def isDebugEnabled: Boolean = logger.isDebugEnabled

  def isTraceEnabled: Boolean = logger.isTraceEnabled

  def debug(msg: => String): Unit = {
    if (logger.isDebugEnabled())
      logger.debug(msgWithLogIdent(msg))
  }
  def debug(e: => Throwable): Any = {
    if (logger.isDebugEnabled())
      logger.debug(logIdent,e)
  }
  def debug(msg: => String, e: => Throwable) = {
    if (logger.isDebugEnabled())
      logger.debug(msgWithLogIdent(msg),e)
  }
  def swallowDebug(action: => Unit) {
    CoreUtils.swallow(logger.debug, action)
  }

  def info(msg: => String): Unit = {
    if (logger.isInfoEnabled())
      logger.info(msgWithLogIdent(msg))
  }
  def info(e: => Throwable): Any = {
    if (logger.isInfoEnabled())
      logger.info(logIdent,e)
  }
  def info(msg: => String,e: => Throwable) = {
    if (logger.isInfoEnabled())
      logger.info(msgWithLogIdent(msg),e)
  }
  def swallowInfo(action: => Unit) {
    CoreUtils.swallow(logger.info, action)
  }

  def warn(msg: => String): Unit = {
    logger.warn(msgWithLogIdent(msg))
  }
  def warn(e: => Throwable): Any = {
    logger.warn(logIdent,e)
  }
  def warn(msg: => String, e: => Throwable) = {
    logger.warn(msgWithLogIdent(msg),e)
  }
  def swallowWarn(action: => Unit) {
    CoreUtils.swallow(logger.warn, action)
  }
  def swallow(action: => Unit) = swallowWarn(action)

  def error(msg: => String): Unit = {
    logger.error(msgWithLogIdent(msg))
  }		
  def error(e: => Throwable): Any = {
    logger.error(logIdent,e)
  }
  def error(msg: => String, e: => Throwable) = {
    logger.error(msgWithLogIdent(msg),e)
  }
  def swallowError(action: => Unit) {
    CoreUtils.swallow(logger.error, action)
  }

  def fatal(msg: => String): Unit = {
    logger.error(Log4jControllerRegistration.FatalMarker, msgWithLogIdent(msg))
  }
  def fatal(e: => Throwable): Any = {
    logger.error(Log4jControllerRegistration.FatalMarker, logIdent, e)
  }
  def fatal(msg: => String, e: => Throwable) = {
    logger.error(Log4jControllerRegistration.FatalMarker, msgWithLogIdent(msg), e)
  }
}
