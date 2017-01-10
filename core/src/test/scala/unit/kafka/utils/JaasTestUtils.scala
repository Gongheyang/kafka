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

import java.io.{File, BufferedWriter, FileWriter}

object JaasTestUtils {

  case class Krb5LoginModule(useKeyTab: Boolean,
                             storeKey: Boolean,
                             keyTab: String,
                             principal: String,
                             debug: Boolean,
                             serviceName: Option[String]) {
    def toJaasModule: JaasModule = {
      JaasModule(
        "com.sun.security.auth.module.Krb5LoginModule",
        debug = debug,
        entries = Map(
          "useKeyTab" -> useKeyTab.toString,
          "storeKey" -> storeKey.toString,
          "keyTab" -> keyTab,
          "principal" -> principal
        ) ++ serviceName.map(s => Map("serviceName" -> s)).getOrElse(Map.empty)
      )
    }
  }

  case class PlainLoginModule(username: String,
                              password: String,
                              debug: Boolean = false,
                              validUsers: Map[String, String] = Map.empty) {
    def toJaasModule: JaasModule = {
      JaasModule(
        "org.apache.kafka.common.security.plain.PlainLoginModule",
        debug = debug,
        entries = Map(
          "username" -> username,
          "password" -> password
        ) ++ validUsers.map { case (user, pass) => s"user_$user" -> pass }
      )
    }
  }

  case class ScramLoginModule(username: String,
                              password: String,
                              debug: Boolean = false,
                              validUsers: Map[String, String] = Map.empty) {
    def toJaasModule: JaasModule = {
      JaasModule(
        "org.apache.kafka.common.security.scram.ScramLoginModule",
        debug = debug,
        entries = Map(
          "username" -> username,
          "password" -> password
        ) ++ validUsers.map { case (user, pass) => s"user_$user" -> pass }
      )
    }
  }

  case class JaasModule(moduleName: String,
                        debug: Boolean,
                        entries: Map[String, String]) {
    override def toString: String = {
      s"""$moduleName required
          |  debug=$debug
          |  ${entries.map { case (k, v) => s"""$k="$v"""" }.mkString("", "\n|  ", ";")}
          |""".stripMargin
    }
  }

  class JaasSection(contextName: String,
                    jaasModule: Seq[JaasModule]) {
    override def toString: String = {
      s"""|$contextName {
          |  ${jaasModule.mkString("\n  ")}
          |};
          |""".stripMargin
    }
  }

  private val ZkServerContextName = "Server"
  private val ZkClientContextName = "Client"
  private val ZkUserSuperPasswd = "adminpasswd"
  private val ZkUser = "fpj"
  private val ZkUserPassword = "fpjsecret"
  private val ZkModule = "org.apache.zookeeper.server.auth.DigestLoginModule"

  private val KafkaServerContextName = "KafkaServer"
  private val KafkaServerPrincipal = "kafka/localhost@EXAMPLE.COM"
  private val KafkaClientContextName = "KafkaClient"
  private val KafkaClientPrincipal = "client@EXAMPLE.COM"
  
  private val KafkaPlainUser = "testuser"
  private val KafkaPlainPassword = "testuser-secret"
  private val KafkaPlainAdmin = "admin"
  private val KafkaPlainAdminPassword = "admin-secret"

  val KafkaScramUser = "scram-user"
  val KafkaScramPassword = "scram-user-secret"
  val KafkaScramAdmin = "scram-admin"
  val KafkaScramAdminPassword = "scram-admin-secret"

  def writeZkFile(): String = {
    val jaasFile = TestUtils.tempFile()
    writeToFile(jaasFile, zkSections)
    jaasFile.getCanonicalPath
  }

  def writeKafkaFile(kafkaServerSaslMechanisms: List[String], kafkaClientSaslMechanisms: List[String], serverKeyTabLocation: Option[File], clientKeyTabLocation: Option[File]): String = {
    val jaasFile = TestUtils.tempFile()
    val kafkaSections = Seq(kafkaServerSection(kafkaServerSaslMechanisms, serverKeyTabLocation), kafkaClientSection(kafkaClientSaslMechanisms, clientKeyTabLocation))
    writeToFile(jaasFile, kafkaSections)
    jaasFile.getCanonicalPath
  }

  def writeZkAndKafkaFiles(kafkaServerSaslMechanisms: List[String], kafkaClientSaslMechanisms: List[String], serverKeyTabLocation: Option[File], clientKeyTabLocation: Option[File]): String = {
    val jaasFile = TestUtils.tempFile()
    val kafkaSections = Seq(kafkaServerSection(kafkaServerSaslMechanisms, serverKeyTabLocation), kafkaClientSection(kafkaClientSaslMechanisms, clientKeyTabLocation))
    writeToFile(jaasFile, kafkaSections ++ zkSections)
    jaasFile.getCanonicalPath
  }

  def clientLoginModule(mechanism: String, keytabLocation: Option[File]): String =
    kafkaClientModule(mechanism, keytabLocation).toString

  private def zkSections: Seq[JaasSection] = Seq(
    new JaasSection(ZkServerContextName, Seq(JaasModule(ZkModule, false, Map("user_super" -> ZkUserSuperPasswd, s"user_$ZkUser" -> ZkUserPassword)))),
    new JaasSection(ZkClientContextName, Seq(JaasModule(ZkModule, false, Map("username" -> ZkUser, "password" -> ZkUserPassword))))
  )

  private def kafkaServerSection(mechanisms: List[String], keytabLocation: Option[File]): JaasSection = {
    val modules = mechanisms.map {
      case "GSSAPI" =>
        Krb5LoginModule(
          useKeyTab = true,
          storeKey = true,
          keyTab = keytabLocation.getOrElse(throw new IllegalArgumentException("Keytab location not specified for GSSAPI")).getAbsolutePath,
          principal = KafkaServerPrincipal,
          debug = true,
          serviceName = Some("kafka")).toJaasModule
      case "PLAIN" =>
        PlainLoginModule(
          KafkaPlainAdmin,
          KafkaPlainAdminPassword,
          debug = false,
          Map(KafkaPlainAdmin -> KafkaPlainAdminPassword, KafkaPlainUser -> KafkaPlainPassword)).toJaasModule
      case "SCRAM-SHA-256" | "SCRAM-SHA-512" =>
        ScramLoginModule(
          KafkaScramAdmin,
          KafkaScramAdminPassword,
          debug = false).toJaasModule
      case mechanism => throw new IllegalArgumentException("Unsupported server mechanism " + mechanism)
    }
    new JaasSection(KafkaServerContextName, modules)
  }

  def kafkaClientModule(mechanism: String, keytabLocation: Option[File]): JaasModule = {
    mechanism match {
      case "GSSAPI" =>
        Krb5LoginModule(
          useKeyTab = true,
          storeKey = true,
          keyTab = keytabLocation.getOrElse(throw new IllegalArgumentException("Keytab location not specified for GSSAPI")).getAbsolutePath,
          principal = KafkaClientPrincipal,
          debug = true,
          serviceName = Some("kafka")
        ).toJaasModule
      case "PLAIN" =>
        PlainLoginModule(
          KafkaPlainUser,
          KafkaPlainPassword
        ).toJaasModule
      case "SCRAM-SHA-256" | "SCRAM-SHA-512" =>
        ScramLoginModule(
          KafkaScramUser,
          KafkaScramPassword
        ).toJaasModule
      case mechanism => throw new IllegalArgumentException("Unsupported client mechanism " + mechanism)
    }
  }

  private def kafkaClientSection(mechanisms: List[String], keytabLocation: Option[File]): JaasSection = {
    new JaasSection(KafkaClientContextName, mechanisms.map(m => kafkaClientModule(m, keytabLocation)))
  }

  private def jaasSectionsToString(jaasSections: Seq[JaasSection]): String =
    jaasSections.mkString

  private def writeToFile(file: File, jaasSections: Seq[JaasSection]) {
    val writer = new BufferedWriter(new FileWriter(file))
    try writer.write(jaasSectionsToString(jaasSections))
    finally writer.close()
  }

}
