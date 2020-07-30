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
package kafka.server

import java.util.{Collections, Properties}

import kafka.admin.{AdminOperationException, AdminUtils}
import kafka.common.TopicAlreadyMarkedForDeletionException
import kafka.log.LogConfig
import kafka.utils.Log4jController
import kafka.metrics.KafkaMetricsGroup
import kafka.server.DynamicConfig.QuotaConfigs
import kafka.utils._
import kafka.zk.{AdminZkClient, KafkaZkClient}
import org.apache.kafka.clients.admin.{AlterConfigOp, ScramMechanism}
import org.apache.kafka.clients.admin.AlterConfigOp.OpType
import org.apache.kafka.common.config.ConfigDef.ConfigKey
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef, ConfigException, ConfigResource, LogLevelConfig}
import org.apache.kafka.common.errors.ThrottlingQuotaExceededException
import org.apache.kafka.common.errors.{ApiException, InvalidConfigurationException, InvalidPartitionsException, InvalidReplicaAssignmentException, InvalidRequestException, ReassignmentInProgressException, TopicExistsException, UnknownTopicOrPartitionException, UnsupportedVersionException}
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.message.AlterUserScramCredentialsResponseData.AlterUserScramCredentialsResult
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsTopic
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic
import org.apache.kafka.common.message.CreateTopicsResponseData.{CreatableTopicConfigs, CreatableTopicResult}
import org.apache.kafka.common.message.{AlterUserScramCredentialsRequestData, AlterUserScramCredentialsResponseData, DescribeConfigsResponseData, DescribeUserScramCredentialsResponseData}
import org.apache.kafka.common.message.DescribeConfigsRequestData.DescribeConfigsResource
import org.apache.kafka.common.message.DescribeUserScramCredentialsResponseData.{CredentialInfo, UserScramCredential}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.server.policy.{AlterConfigPolicy, CreateTopicPolicy}
import org.apache.kafka.server.policy.CreateTopicPolicy.RequestMetadata
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.quota.{ClientQuotaAlteration, ClientQuotaEntity, ClientQuotaFilter, ClientQuotaFilterComponent}
import org.apache.kafka.common.requests.CreateTopicsRequest._
import org.apache.kafka.common.requests.DescribeConfigsResponse.ConfigSource
import org.apache.kafka.common.requests.{AlterConfigsRequest, ApiError, DescribeConfigsResponse}
import org.apache.kafka.common.security.scram.internals.ScramCredentialUtils
import org.apache.kafka.common.utils.Sanitizer

import scala.collection.{Map, mutable, _}
import scala.jdk.CollectionConverters._

class AdminManager(val config: KafkaConfig,
                   val metrics: Metrics,
                   val metadataCache: MetadataCache,
                   val zkClient: KafkaZkClient) extends Logging with KafkaMetricsGroup {

  this.logIdent = "[Admin Manager on Broker " + config.brokerId + "]: "

  private val topicPurgatory = DelayedOperationPurgatory[DelayedOperation]("topic", config.brokerId)
  private val adminZkClient = new AdminZkClient(zkClient)

  private val createTopicPolicy =
    Option(config.getConfiguredInstance(KafkaConfig.CreateTopicPolicyClassNameProp, classOf[CreateTopicPolicy]))

  private val alterConfigPolicy =
    Option(config.getConfiguredInstance(KafkaConfig.AlterConfigPolicyClassNameProp, classOf[AlterConfigPolicy]))

  def hasDelayedTopicOperations = topicPurgatory.numDelayed != 0

  private val defaultNumPartitions = config.numPartitions.intValue()
  private val defaultReplicationFactor = config.defaultReplicationFactor.shortValue()

  /**
    * Try to complete delayed topic operations with the request key
    */
  def tryCompleteDelayedTopicOperations(topic: String): Unit = {
    val key = TopicKey(topic)
    val completed = topicPurgatory.checkAndComplete(key)
    debug(s"Request key ${key.keyLabel} unblocked $completed topic requests.")
  }

  private def validateTopicCreatePolicy(topic: CreatableTopic,
                                        resolvedNumPartitions: Int,
                                        resolvedReplicationFactor: Short,
                                        assignments: Map[Int, Seq[Int]]): Unit = {
    createTopicPolicy.foreach { policy =>
      // Use `null` for unset fields in the public API
      val numPartitions: java.lang.Integer =
        if (topic.assignments().isEmpty) resolvedNumPartitions else null
      val replicationFactor: java.lang.Short =
        if (topic.assignments().isEmpty) resolvedReplicationFactor else null
      val javaAssignments = if (topic.assignments().isEmpty) {
        null
      } else {
        assignments.map { case (k, v) =>
          (k: java.lang.Integer) -> v.map(i => i: java.lang.Integer).asJava
        }.asJava
      }
      val javaConfigs = new java.util.HashMap[String, String]
      topic.configs.forEach(config => javaConfigs.put(config.name, config.value))
      policy.validate(new RequestMetadata(topic.name, numPartitions, replicationFactor,
        javaAssignments, javaConfigs))
    }
  }

  private def maybePopulateMetadataAndConfigs(metadataAndConfigs: Map[String, CreatableTopicResult],
                                              topicName: String,
                                              configs: Properties,
                                              assignments: Map[Int, Seq[Int]]): Unit = {
    metadataAndConfigs.get(topicName).foreach { result =>
      val logConfig = LogConfig.fromProps(KafkaServer.copyKafkaConfigToLog(config), configs)
      val createEntry = createTopicConfigEntry(logConfig, configs, includeSynonyms = false, includeDocumentation = false)(_, _)
      val topicConfigs = logConfig.values.asScala.map { case (k, v) =>
        val entry = createEntry(k, v)
        new CreatableTopicConfigs()
          .setName(k)
          .setValue(entry.value)
          .setIsSensitive(entry.isSensitive)
          .setReadOnly(entry.readOnly)
          .setConfigSource(entry.configSource)
      }.toList.asJava
      result.setConfigs(topicConfigs)
      result.setNumPartitions(assignments.size)
      result.setReplicationFactor(assignments(0).size.toShort)
    }
  }

  /**
    * Create topics and wait until the topics have been completely created.
    * The callback function will be triggered either when timeout, error or the topics are created.
    */
  def createTopics(timeout: Int,
                   validateOnly: Boolean,
                   toCreate: Map[String, CreatableTopic],
                   includeConfigsAndMetadata: Map[String, CreatableTopicResult],
                   controllerMutationQuota: ControllerMutationQuota,
                   responseCallback: Map[String, ApiError] => Unit): Unit = {

    // 1. map over topics creating assignment and calling zookeeper
    val brokers = metadataCache.getAliveBrokers.map { b => kafka.admin.BrokerMetadata(b.id, b.rack) }
    val metadata = toCreate.values.map(topic =>
      try {
        if (metadataCache.contains(topic.name))
          throw new TopicExistsException(s"Topic '${topic.name}' already exists.")

        val nullConfigs = topic.configs.asScala.filter(_.value == null).map(_.name)
        if (nullConfigs.nonEmpty)
          throw new InvalidRequestException(s"Null value not supported for topic configs : ${nullConfigs.mkString(",")}")

        if ((topic.numPartitions != NO_NUM_PARTITIONS || topic.replicationFactor != NO_REPLICATION_FACTOR)
            && !topic.assignments().isEmpty) {
          throw new InvalidRequestException("Both numPartitions or replicationFactor and replicasAssignments were set. " +
            "Both cannot be used at the same time.")
        }

        val resolvedNumPartitions = if (topic.numPartitions == NO_NUM_PARTITIONS)
          defaultNumPartitions else topic.numPartitions
        val resolvedReplicationFactor = if (topic.replicationFactor == NO_REPLICATION_FACTOR)
          defaultReplicationFactor else topic.replicationFactor

        val assignments = if (topic.assignments.isEmpty) {
          AdminUtils.assignReplicasToBrokers(
            brokers, resolvedNumPartitions, resolvedReplicationFactor)
        } else {
          val assignments = new mutable.HashMap[Int, Seq[Int]]
          // Note: we don't check that replicaAssignment contains unknown brokers - unlike in add-partitions case,
          // this follows the existing logic in TopicCommand
          topic.assignments.forEach { assignment =>
            assignments(assignment.partitionIndex) = assignment.brokerIds.asScala.map(a => a: Int)
          }
          assignments
        }
        trace(s"Assignments for topic $topic are $assignments ")

        val configs = new Properties()
        topic.configs.forEach(entry => configs.setProperty(entry.name, entry.value))
        adminZkClient.validateTopicCreate(topic.name, assignments, configs)
        validateTopicCreatePolicy(topic, resolvedNumPartitions, resolvedReplicationFactor, assignments)

        // For responses with DescribeConfigs permission, populate metadata and configs. It is
        // safe to populate it before creating the topic because the values are unset if the
        // creation fails.
        maybePopulateMetadataAndConfigs(includeConfigsAndMetadata, topic.name, configs, assignments)

        if (validateOnly) {
          CreatePartitionsMetadata(topic.name, assignments.keySet)
        } else {
          controllerMutationQuota.record(assignments.size)
          adminZkClient.createTopicWithAssignment(topic.name, configs, assignments, validate = false)
          CreatePartitionsMetadata(topic.name, assignments.keySet)
        }
      } catch {
        // Log client errors at a lower level than unexpected exceptions
        case e: TopicExistsException =>
          debug(s"Topic creation failed since topic '${topic.name}' already exists.", e)
          CreatePartitionsMetadata(topic.name, e)
        case e: ThrottlingQuotaExceededException =>
          debug(s"Topic creation not allowed because quota is violated. Delay time: ${e.throttleTimeMs}")
          CreatePartitionsMetadata(topic.name, e)
        case e: ApiException =>
          info(s"Error processing create topic request $topic", e)
          CreatePartitionsMetadata(topic.name, e)
        case e: ConfigException =>
          info(s"Error processing create topic request $topic", e)
          CreatePartitionsMetadata(topic.name, new InvalidConfigurationException(e.getMessage, e.getCause))
        case e: Throwable =>
          error(s"Error processing create topic request $topic", e)
          CreatePartitionsMetadata(topic.name, e)
      }).toBuffer

    // 2. if timeout <= 0, validateOnly or no topics can proceed return immediately
    if (timeout <= 0 || validateOnly || !metadata.exists(_.error.is(Errors.NONE))) {
      val results = metadata.map { createTopicMetadata =>
        // ignore topics that already have errors
        if (createTopicMetadata.error.isSuccess && !validateOnly) {
          (createTopicMetadata.topic, new ApiError(Errors.REQUEST_TIMED_OUT, null))
        } else {
          (createTopicMetadata.topic, createTopicMetadata.error)
        }
      }.toMap
      responseCallback(results)
    } else {
      // 3. else pass the assignments and errors to the delayed operation and set the keys
      val delayedCreate = new DelayedCreatePartitions(timeout, metadata, this,
        responseCallback)
      val delayedCreateKeys = toCreate.values.map(topic => TopicKey(topic.name)).toBuffer
      // try to complete the request immediately, otherwise put it into the purgatory
      topicPurgatory.tryCompleteElseWatch(delayedCreate, delayedCreateKeys)
    }
  }

  /**
    * Delete topics and wait until the topics have been completely deleted.
    * The callback function will be triggered either when timeout, error or the topics are deleted.
    */
  def deleteTopics(timeout: Int,
                   topics: Set[String],
                   controllerMutationQuota: ControllerMutationQuota,
                   responseCallback: Map[String, Errors] => Unit): Unit = {
    // 1. map over topics calling the asynchronous delete
    val metadata = topics.map { topic =>
        try {
          controllerMutationQuota.record(metadataCache.numPartitions(topic).getOrElse(0).toDouble)
          adminZkClient.deleteTopic(topic)
          DeleteTopicMetadata(topic, Errors.NONE)
        } catch {
          case _: TopicAlreadyMarkedForDeletionException =>
            // swallow the exception, and still track deletion allowing multiple calls to wait for deletion
            DeleteTopicMetadata(topic, Errors.NONE)
          case e: ThrottlingQuotaExceededException =>
            debug(s"Topic deletion not allowed because quota is violated. Delay time: ${e.throttleTimeMs}")
            DeleteTopicMetadata(topic, e)
          case e: Throwable =>
            error(s"Error processing delete topic request for topic $topic", e)
            DeleteTopicMetadata(topic, e)
        }
    }

    // 2. if timeout <= 0 or no topics can proceed return immediately
    if (timeout <= 0 || !metadata.exists(_.error == Errors.NONE)) {
      val results = metadata.map { deleteTopicMetadata =>
        // ignore topics that already have errors
        if (deleteTopicMetadata.error == Errors.NONE) {
          (deleteTopicMetadata.topic, Errors.REQUEST_TIMED_OUT)
        } else {
          (deleteTopicMetadata.topic, deleteTopicMetadata.error)
        }
      }.toMap
      responseCallback(results)
    } else {
      // 3. else pass the topics and errors to the delayed operation and set the keys
      val delayedDelete = new DelayedDeleteTopics(timeout, metadata.toSeq, this, responseCallback)
      val delayedDeleteKeys = topics.map(TopicKey).toSeq
      // try to complete the request immediately, otherwise put it into the purgatory
      topicPurgatory.tryCompleteElseWatch(delayedDelete, delayedDeleteKeys)
    }
  }

  def createPartitions(timeout: Int,
                       newPartitions: Seq[CreatePartitionsTopic],
                       validateOnly: Boolean,
                       controllerMutationQuota: ControllerMutationQuota,
                       callback: Map[String, ApiError] => Unit): Unit = {
    val allBrokers = adminZkClient.getBrokerMetadatas()
    val allBrokerIds = allBrokers.map(_.id)

    // 1. map over topics creating assignment and calling AdminUtils
    val metadata = newPartitions.map { newPartition =>
      val topic = newPartition.name

      try {
        val existingAssignment = zkClient.getFullReplicaAssignmentForTopics(immutable.Set(topic)).map {
          case (topicPartition, assignment) =>
            if (assignment.isBeingReassigned) {
              // We prevent adding partitions while topic reassignment is in progress, to protect from a race condition
              // between the controller thread processing reassignment update and createPartitions(this) request.
              throw new ReassignmentInProgressException(s"A partition reassignment is in progress for the topic '$topic'.")
            }
            topicPartition.partition -> assignment
        }
        if (existingAssignment.isEmpty)
          throw new UnknownTopicOrPartitionException(s"The topic '$topic' does not exist.")

        val oldNumPartitions = existingAssignment.size
        val newNumPartitions = newPartition.count
        val numPartitionsIncrement = newNumPartitions - oldNumPartitions
        if (numPartitionsIncrement < 0) {
          throw new InvalidPartitionsException(
            s"Topic currently has $oldNumPartitions partitions, which is higher than the requested $newNumPartitions.")
        } else if (numPartitionsIncrement == 0) {
          throw new InvalidPartitionsException(s"Topic already has $oldNumPartitions partitions.")
        }

        val newPartitionsAssignment = Option(newPartition.assignments).map { assignmentMap =>
          val assignments = assignmentMap.asScala.map {
            createPartitionAssignment => createPartitionAssignment.brokerIds.asScala.map(_.toInt)
          }
          val unknownBrokers = assignments.flatten.toSet -- allBrokerIds
          if (unknownBrokers.nonEmpty)
            throw new InvalidReplicaAssignmentException(
              s"Unknown broker(s) in replica assignment: ${unknownBrokers.mkString(", ")}.")

          if (assignments.size != numPartitionsIncrement)
            throw new InvalidReplicaAssignmentException(
              s"Increasing the number of partitions by $numPartitionsIncrement " +
                s"but ${assignments.size} assignments provided.")

          assignments.zipWithIndex.map { case (replicas, index) =>
            existingAssignment.size + index -> replicas
          }.toMap
        }

        val assignmentForNewPartitions = adminZkClient.createNewPartitionsAssignment(
          topic, existingAssignment, allBrokers, newPartition.count, newPartitionsAssignment)

        if (validateOnly) {
          CreatePartitionsMetadata(topic, (existingAssignment ++ assignmentForNewPartitions).keySet)
        } else {
          controllerMutationQuota.record(numPartitionsIncrement)
          val updatedReplicaAssignment = adminZkClient.createPartitionsWithAssignment(
            topic, existingAssignment, assignmentForNewPartitions)
          CreatePartitionsMetadata(topic, updatedReplicaAssignment.keySet)
        }
      } catch {
        case e: AdminOperationException =>
          CreatePartitionsMetadata(topic, e)
        case e: ThrottlingQuotaExceededException =>
          debug(s"Partition(s) creation not allowed because quota is violated. Delay time: ${e.throttleTimeMs}")
          CreatePartitionsMetadata(topic, e)
        case e: ApiException =>
          CreatePartitionsMetadata(topic, e)
      }
    }

    // 2. if timeout <= 0, validateOnly or no topics can proceed return immediately
    if (timeout <= 0 || validateOnly || !metadata.exists(_.error.is(Errors.NONE))) {
      val results = metadata.map { createPartitionMetadata =>
        // ignore topics that already have errors
        if (createPartitionMetadata.error.isSuccess && !validateOnly) {
          (createPartitionMetadata.topic, new ApiError(Errors.REQUEST_TIMED_OUT, null))
        } else {
          (createPartitionMetadata.topic, createPartitionMetadata.error)
        }
      }.toMap
      callback(results)
    } else {
      // 3. else pass the assignments and errors to the delayed operation and set the keys
      val delayedCreate = new DelayedCreatePartitions(timeout, metadata, this, callback)
      val delayedCreateKeys = newPartitions.map(createPartitionTopic => TopicKey(createPartitionTopic.name))
      // try to complete the request immediately, otherwise put it into the purgatory
      topicPurgatory.tryCompleteElseWatch(delayedCreate, delayedCreateKeys)
    }
  }

  def describeConfigs(resourceToConfigNames: List[DescribeConfigsResource],
                      includeSynonyms: Boolean,
                      includeDocumentation: Boolean): List[DescribeConfigsResponseData.DescribeConfigsResult] = {
    resourceToConfigNames.map { case resource =>

      def allConfigs(config: AbstractConfig) = {
        config.originals.asScala.filter(_._2 != null) ++ config.values.asScala
      }
      def createResponseConfig(configs: Map[String, Any],
                               createConfigEntry: (String, Any) => DescribeConfigsResponseData.DescribeConfigsResourceResult): DescribeConfigsResponseData.DescribeConfigsResult = {
        val filteredConfigPairs = if (resource.configurationKeys == null)
          configs.toBuffer
        else
          configs.filter { case (configName, _) =>
            resource.configurationKeys.asScala.forall(_.contains(configName))
          }.toBuffer

        val configEntries = filteredConfigPairs.map { case (name, value) => createConfigEntry(name, value) }
        new DescribeConfigsResponseData.DescribeConfigsResult().setErrorCode(Errors.NONE.code)
          .setConfigs(configEntries.asJava)
      }

      try {
        val configResult = ConfigResource.Type.forId(resource.resourceType) match {
          case ConfigResource.Type.TOPIC =>
            val topic = resource.resourceName
            Topic.validate(topic)
            if (metadataCache.contains(topic)) {
              // Consider optimizing this by caching the configs or retrieving them from the `Log` when possible
              val topicProps = adminZkClient.fetchEntityConfig(ConfigType.Topic, topic)
              val logConfig = LogConfig.fromProps(KafkaServer.copyKafkaConfigToLog(config), topicProps)
              createResponseConfig(allConfigs(logConfig), createTopicConfigEntry(logConfig, topicProps, includeSynonyms, includeDocumentation))
            } else {
              new DescribeConfigsResponseData.DescribeConfigsResult().setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
                .setConfigs(Collections.emptyList[DescribeConfigsResponseData.DescribeConfigsResourceResult])
            }

          case ConfigResource.Type.BROKER =>
            if (resource.resourceName == null || resource.resourceName.isEmpty)
              createResponseConfig(config.dynamicConfig.currentDynamicDefaultConfigs,
                  createBrokerConfigEntry(perBrokerConfig = false, includeSynonyms, includeDocumentation))
            else if (resourceNameToBrokerId(resource.resourceName) == config.brokerId)
              createResponseConfig(allConfigs(config),
                  createBrokerConfigEntry(perBrokerConfig = true, includeSynonyms, includeDocumentation))
            else
              throw new InvalidRequestException(s"Unexpected broker id, expected ${config.brokerId} or empty string, but received ${resource.resourceName}")

          case ConfigResource.Type.BROKER_LOGGER =>
            if (resource.resourceName == null || resource.resourceName.isEmpty)
              throw new InvalidRequestException("Broker id must not be empty")
            else if (resourceNameToBrokerId(resource.resourceName) != config.brokerId)
              throw new InvalidRequestException(s"Unexpected broker id, expected ${config.brokerId} but received ${resource.resourceName}")
            else
              createResponseConfig(Log4jController.loggers,
                (name, value) => new DescribeConfigsResponseData.DescribeConfigsResourceResult().setName(name)
                  .setValue(value.toString).setConfigSource(ConfigSource.DYNAMIC_BROKER_LOGGER_CONFIG.id)
                  .setIsSensitive(false).setReadOnly(false).setSynonyms(List.empty.asJava))
          case resourceType => throw new InvalidRequestException(s"Unsupported resource type: $resourceType")
        }
        configResult.setResourceName(resource.resourceName).setResourceType(resource.resourceType)
      } catch {
        case e: Throwable =>
          // Log client errors at a lower level than unexpected exceptions
          val message = s"Error processing describe configs request for resource $resource"
          if (e.isInstanceOf[ApiException])
            info(message, e)
          else
            error(message, e)
          val err = ApiError.fromThrowable(e)
          new DescribeConfigsResponseData.DescribeConfigsResult()
              .setResourceName(resource.resourceName)
              .setResourceType(resource.resourceType)
              .setErrorMessage(err.message)
              .setErrorCode(err.error.code)
              .setConfigs(Collections.emptyList[DescribeConfigsResponseData.DescribeConfigsResourceResult])
      }
    }.toList
  }

  def alterConfigs(configs: Map[ConfigResource, AlterConfigsRequest.Config], validateOnly: Boolean): Map[ConfigResource, ApiError] = {
    configs.map { case (resource, config) =>

      try {
        val nullUpdates = config.entries.asScala.filter(_.value == null).map(_.name)
        if (nullUpdates.nonEmpty)
          throw new InvalidRequestException(s"Null value not supported for : ${nullUpdates.mkString(",")}")

        val configEntriesMap = config.entries.asScala.map(entry => (entry.name, entry.value)).toMap

        val configProps = new Properties
        config.entries.asScala.filter(_.value != null).foreach { configEntry =>
          configProps.setProperty(configEntry.name, configEntry.value)
        }

        resource.`type` match {
          case ConfigResource.Type.TOPIC => alterTopicConfigs(resource, validateOnly, configProps, configEntriesMap)
          case ConfigResource.Type.BROKER => alterBrokerConfigs(resource, validateOnly, configProps, configEntriesMap)
          case resourceType =>
            throw new InvalidRequestException(s"AlterConfigs is only supported for topics and brokers, but resource type is $resourceType")
        }
      } catch {
        case e @ (_: ConfigException | _: IllegalArgumentException) =>
          val message = s"Invalid config value for resource $resource: ${e.getMessage}"
          info(message)
          resource -> ApiError.fromThrowable(new InvalidRequestException(message, e))
        case e: Throwable =>
          // Log client errors at a lower level than unexpected exceptions
          val message = s"Error processing alter configs request for resource $resource, config $config"
          if (e.isInstanceOf[ApiException])
            info(message, e)
          else
            error(message, e)
          resource -> ApiError.fromThrowable(e)
      }
    }.toMap
  }

  private def alterTopicConfigs(resource: ConfigResource, validateOnly: Boolean,
                                configProps: Properties, configEntriesMap: Map[String, String]): (ConfigResource, ApiError) = {
    val topic = resource.name
    if (!metadataCache.contains(topic))
      throw new UnknownTopicOrPartitionException(s"The topic '$topic' does not exist.")

    adminZkClient.validateTopicConfig(topic, configProps)
    validateConfigPolicy(resource, configEntriesMap)
    if (!validateOnly) {
      info(s"Updating topic $topic with new configuration $config")
      adminZkClient.changeTopicConfig(topic, configProps)
    }

    resource -> ApiError.NONE
  }

  private def alterBrokerConfigs(resource: ConfigResource, validateOnly: Boolean,
                                 configProps: Properties, configEntriesMap: Map[String, String]): (ConfigResource, ApiError) = {
    val brokerId = getBrokerId(resource)
    val perBrokerConfig = brokerId.nonEmpty
    this.config.dynamicConfig.validate(configProps, perBrokerConfig)
    validateConfigPolicy(resource, configEntriesMap)
    if (!validateOnly) {
      if (perBrokerConfig)
        this.config.dynamicConfig.reloadUpdatedFilesWithoutConfigChange(configProps)
      adminZkClient.changeBrokerConfig(brokerId,
        this.config.dynamicConfig.toPersistentProps(configProps, perBrokerConfig))
    }

    resource -> ApiError.NONE
  }

  private def alterLogLevelConfigs(alterConfigOps: Seq[AlterConfigOp]): Unit = {
    alterConfigOps.foreach { alterConfigOp =>
      val loggerName = alterConfigOp.configEntry().name()
      val logLevel = alterConfigOp.configEntry().value()
      alterConfigOp.opType() match {
        case OpType.SET => Log4jController.logLevel(loggerName, logLevel)
        case OpType.DELETE => Log4jController.unsetLogLevel(loggerName)
        case _ => throw new IllegalArgumentException(
          s"Log level cannot be changed for OpType: ${alterConfigOp.opType()}")
      }
    }
  }

  private def getBrokerId(resource: ConfigResource) = {
    if (resource.name == null || resource.name.isEmpty)
      None
    else {
      val id = resourceNameToBrokerId(resource.name)
      if (id != this.config.brokerId)
        throw new InvalidRequestException(s"Unexpected broker id, expected ${this.config.brokerId}, but received ${resource.name}")
      Some(id)
    }
  }

  private def validateConfigPolicy(resource: ConfigResource, configEntriesMap: Map[String, String]): Unit = {
    alterConfigPolicy match {
      case Some(policy) =>
        policy.validate(new AlterConfigPolicy.RequestMetadata(
          new ConfigResource(resource.`type`(), resource.name), configEntriesMap.asJava))
      case None =>
    }
  }

  def incrementalAlterConfigs(configs: Map[ConfigResource, Seq[AlterConfigOp]], validateOnly: Boolean): Map[ConfigResource, ApiError] = {
    configs.map { case (resource, alterConfigOps) =>
      try {
        // throw InvalidRequestException if any duplicate keys
        val duplicateKeys = alterConfigOps.groupBy(config => config.configEntry.name).filter { case (_, v) =>
          v.size > 1
        }.keySet
        if (duplicateKeys.nonEmpty)
          throw new InvalidRequestException(s"Error due to duplicate config keys : ${duplicateKeys.mkString(",")}")
        val nullUpdates = alterConfigOps
          .filter(entry => entry.configEntry.value == null && entry.opType() != OpType.DELETE)
          .map(entry => s"${entry.opType}:${entry.configEntry.name}")
        if (nullUpdates.nonEmpty)
          throw new InvalidRequestException(s"Null value not supported for : ${nullUpdates.mkString(",")}")

        val configEntriesMap = alterConfigOps.map(entry => (entry.configEntry.name, entry.configEntry.value)).toMap

        resource.`type` match {
          case ConfigResource.Type.TOPIC =>
            val configProps = adminZkClient.fetchEntityConfig(ConfigType.Topic, resource.name)
            prepareIncrementalConfigs(alterConfigOps, configProps, LogConfig.configKeys)
            alterTopicConfigs(resource, validateOnly, configProps, configEntriesMap)

          case ConfigResource.Type.BROKER =>
            val brokerId = getBrokerId(resource)
            val perBrokerConfig = brokerId.nonEmpty

            val persistentProps = if (perBrokerConfig) adminZkClient.fetchEntityConfig(ConfigType.Broker, brokerId.get.toString)
            else adminZkClient.fetchEntityConfig(ConfigType.Broker, ConfigEntityName.Default)

            val configProps = this.config.dynamicConfig.fromPersistentProps(persistentProps, perBrokerConfig)
            prepareIncrementalConfigs(alterConfigOps, configProps, KafkaConfig.configKeys)
            alterBrokerConfigs(resource, validateOnly, configProps, configEntriesMap)

          case ConfigResource.Type.BROKER_LOGGER =>
            getBrokerId(resource)
            validateLogLevelConfigs(alterConfigOps)

            if (!validateOnly)
              alterLogLevelConfigs(alterConfigOps)
            resource -> ApiError.NONE
          case resourceType =>
            throw new InvalidRequestException(s"AlterConfigs is only supported for topics and brokers, but resource type is $resourceType")
        }
      } catch {
        case e @ (_: ConfigException | _: IllegalArgumentException) =>
          val message = s"Invalid config value for resource $resource: ${e.getMessage}"
          info(message)
          resource -> ApiError.fromThrowable(new InvalidRequestException(message, e))
        case e: Throwable =>
          // Log client errors at a lower level than unexpected exceptions
          val message = s"Error processing alter configs request for resource $resource, config $alterConfigOps"
          if (e.isInstanceOf[ApiException])
            info(message, e)
          else
            error(message, e)
          resource -> ApiError.fromThrowable(e)
      }
    }.toMap
  }

  private def validateLogLevelConfigs(alterConfigOps: Seq[AlterConfigOp]): Unit = {
    def validateLoggerNameExists(loggerName: String): Unit = {
      if (!Log4jController.loggerExists(loggerName))
        throw new ConfigException(s"Logger $loggerName does not exist!")
    }

    alterConfigOps.foreach { alterConfigOp =>
      val loggerName = alterConfigOp.configEntry.name
      alterConfigOp.opType() match {
        case OpType.SET =>
          validateLoggerNameExists(loggerName)
          val logLevel = alterConfigOp.configEntry.value
          if (!LogLevelConfig.VALID_LOG_LEVELS.contains(logLevel)) {
            val validLevelsStr = LogLevelConfig.VALID_LOG_LEVELS.asScala.mkString(", ")
            throw new ConfigException(
              s"Cannot set the log level of $loggerName to $logLevel as it is not a supported log level. " +
              s"Valid log levels are $validLevelsStr"
            )
          }
        case OpType.DELETE =>
          validateLoggerNameExists(loggerName)
          if (loggerName == Log4jController.ROOT_LOGGER)
            throw new InvalidRequestException(s"Removing the log level of the ${Log4jController.ROOT_LOGGER} logger is not allowed")
        case OpType.APPEND => throw new InvalidRequestException(s"${OpType.APPEND} operation is not allowed for the ${ConfigResource.Type.BROKER_LOGGER} resource")
        case OpType.SUBTRACT => throw new InvalidRequestException(s"${OpType.SUBTRACT} operation is not allowed for the ${ConfigResource.Type.BROKER_LOGGER} resource")
      }
    }
  }

  private def prepareIncrementalConfigs(alterConfigOps: Seq[AlterConfigOp], configProps: Properties, configKeys: Map[String, ConfigKey]): Unit = {

    def listType(configName: String, configKeys: Map[String, ConfigKey]): Boolean = {
      val configKey = configKeys(configName)
      if (configKey == null)
        throw new InvalidConfigurationException(s"Unknown topic config name: $configName")
      configKey.`type` == ConfigDef.Type.LIST
    }

    alterConfigOps.foreach { alterConfigOp =>
      val configPropName = alterConfigOp.configEntry.name
      alterConfigOp.opType() match {
        case OpType.SET => configProps.setProperty(alterConfigOp.configEntry.name, alterConfigOp.configEntry.value)
        case OpType.DELETE => configProps.remove(alterConfigOp.configEntry.name)
        case OpType.APPEND => {
          if (!listType(alterConfigOp.configEntry.name, configKeys))
            throw new InvalidRequestException(s"Config value append is not allowed for config key: ${alterConfigOp.configEntry.name}")
          val oldValueList = Option(configProps.getProperty(alterConfigOp.configEntry.name))
            .orElse(Option(ConfigDef.convertToString(configKeys(configPropName).defaultValue, ConfigDef.Type.LIST)))
            .getOrElse("")
            .split(",").toList
          val newValueList = oldValueList ::: alterConfigOp.configEntry.value.split(",").toList
          configProps.setProperty(alterConfigOp.configEntry.name, newValueList.mkString(","))
        }
        case OpType.SUBTRACT => {
          if (!listType(alterConfigOp.configEntry.name, configKeys))
            throw new InvalidRequestException(s"Config value subtract is not allowed for config key: ${alterConfigOp.configEntry.name}")
          val oldValueList = Option(configProps.getProperty(alterConfigOp.configEntry.name))
            .orElse(Option(ConfigDef.convertToString(configKeys(configPropName).defaultValue, ConfigDef.Type.LIST)))
            .getOrElse("")
            .split(",").toList
          val newValueList = oldValueList.diff(alterConfigOp.configEntry.value.split(",").toList)
          configProps.setProperty(alterConfigOp.configEntry.name, newValueList.mkString(","))
        }
      }
    }
  }

  def shutdown(): Unit = {
    topicPurgatory.shutdown()
    CoreUtils.swallow(createTopicPolicy.foreach(_.close()), this)
    CoreUtils.swallow(alterConfigPolicy.foreach(_.close()), this)
  }

  private def resourceNameToBrokerId(resourceName: String): Int = {
    try resourceName.toInt catch {
      case _: NumberFormatException =>
        throw new InvalidRequestException(s"Broker id must be an integer, but it is: $resourceName")
    }
  }

  private def brokerSynonyms(name: String): List[String] = {
    DynamicBrokerConfig.brokerConfigSynonyms(name, matchListenerOverride = true)
  }

  private def brokerDocumentation(name: String): String = {
    config.documentationOf(name)
  }

  private def configResponseType(configType: Option[ConfigDef.Type]): DescribeConfigsResponse.ConfigType = {
    if (configType.isEmpty)
      DescribeConfigsResponse.ConfigType.UNKNOWN
    else configType.get match {
      case ConfigDef.Type.BOOLEAN => DescribeConfigsResponse.ConfigType.BOOLEAN
      case ConfigDef.Type.STRING => DescribeConfigsResponse.ConfigType.STRING
      case ConfigDef.Type.INT => DescribeConfigsResponse.ConfigType.INT
      case ConfigDef.Type.SHORT => DescribeConfigsResponse.ConfigType.SHORT
      case ConfigDef.Type.LONG => DescribeConfigsResponse.ConfigType.LONG
      case ConfigDef.Type.DOUBLE => DescribeConfigsResponse.ConfigType.DOUBLE
      case ConfigDef.Type.LIST => DescribeConfigsResponse.ConfigType.LIST
      case ConfigDef.Type.CLASS => DescribeConfigsResponse.ConfigType.CLASS
      case ConfigDef.Type.PASSWORD => DescribeConfigsResponse.ConfigType.PASSWORD
      case _ => DescribeConfigsResponse.ConfigType.UNKNOWN
    }
  }
  
  private def configSynonyms(name: String, synonyms: List[String], isSensitive: Boolean): List[DescribeConfigsResponseData.DescribeConfigsSynonym] = {
    val dynamicConfig = config.dynamicConfig
    val allSynonyms = mutable.Buffer[DescribeConfigsResponseData.DescribeConfigsSynonym]()

    def maybeAddSynonym(map: Map[String, String], source: ConfigSource)(name: String): Unit = {
      map.get(name).map { value =>
        val configValue = if (isSensitive) null else value
        allSynonyms += new DescribeConfigsResponseData.DescribeConfigsSynonym().setName(name).setValue(configValue).setSource(source.id)
      }
    }

    synonyms.foreach(maybeAddSynonym(dynamicConfig.currentDynamicBrokerConfigs, ConfigSource.DYNAMIC_BROKER_CONFIG))
    synonyms.foreach(maybeAddSynonym(dynamicConfig.currentDynamicDefaultConfigs, ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG))
    synonyms.foreach(maybeAddSynonym(dynamicConfig.staticBrokerConfigs, ConfigSource.STATIC_BROKER_CONFIG))
    synonyms.foreach(maybeAddSynonym(dynamicConfig.staticDefaultConfigs, ConfigSource.DEFAULT_CONFIG))
    allSynonyms.dropWhile(s => s.name != name).toList // e.g. drop listener overrides when describing base config
  }

  private def createTopicConfigEntry(logConfig: LogConfig, topicProps: Properties, includeSynonyms: Boolean, includeDocumentation: Boolean)
                                    (name: String, value: Any): DescribeConfigsResponseData.DescribeConfigsResourceResult = {
    val configEntryType = LogConfig.configType(name)
    val isSensitive = KafkaConfig.maybeSensitive(configEntryType)
    val valueAsString = if (isSensitive) null else ConfigDef.convertToString(value, configEntryType.orNull)
    val allSynonyms = {
      val list = LogConfig.TopicConfigSynonyms.get(name)
        .map(s => configSynonyms(s, brokerSynonyms(s), isSensitive))
        .getOrElse(List.empty)
      if (!topicProps.containsKey(name))
        list
      else
        new DescribeConfigsResponseData.DescribeConfigsSynonym().setName(name).setValue(valueAsString)
          .setSource(ConfigSource.TOPIC_CONFIG.id) +: list
    }
    val source = if (allSynonyms.isEmpty) ConfigSource.DEFAULT_CONFIG.id else allSynonyms.head.source
    val synonyms = if (!includeSynonyms) List.empty else allSynonyms
    val dataType = configResponseType(configEntryType)
    val configDocumentation = if (includeDocumentation) brokerDocumentation(name) else null
    new DescribeConfigsResponseData.DescribeConfigsResourceResult()
      .setName(name).setValue(valueAsString).setConfigSource(source)
      .setIsSensitive(isSensitive).setReadOnly(false).setSynonyms(synonyms.asJava)
      .setDocumentation(configDocumentation).setConfigType(dataType.id)
  }

  private def createBrokerConfigEntry(perBrokerConfig: Boolean, includeSynonyms: Boolean, includeDocumentation: Boolean)
                                     (name: String, value: Any): DescribeConfigsResponseData.DescribeConfigsResourceResult = {
    val allNames = brokerSynonyms(name)
    val configEntryType = KafkaConfig.configType(name)
    val isSensitive = KafkaConfig.maybeSensitive(configEntryType)
    val valueAsString = if (isSensitive)
      null
    else value match {
      case v: String => v
      case _ => ConfigDef.convertToString(value, configEntryType.orNull)
    }
    val allSynonyms = configSynonyms(name, allNames, isSensitive)
        .filter(perBrokerConfig || _.source == ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG.id)
    val synonyms = if (!includeSynonyms) List.empty else allSynonyms
    val source = if (allSynonyms.isEmpty) ConfigSource.DEFAULT_CONFIG.id else allSynonyms.head.source
    val readOnly = !DynamicBrokerConfig.AllDynamicConfigs.contains(name)

    val dataType = configResponseType(configEntryType)
    val configDocumentation = if (includeDocumentation) brokerDocumentation(name) else null
    new DescribeConfigsResponseData.DescribeConfigsResourceResult().setName(name).setValue(valueAsString).setConfigSource(source)
      .setIsSensitive(isSensitive).setReadOnly(readOnly).setSynonyms(synonyms.asJava)
      .setDocumentation(configDocumentation).setConfigType(dataType.id)
  }

  private def sanitizeEntityName(entityName: String): String =
    Option(entityName) match {
      case None => ConfigEntityName.Default
      case Some(name) => Sanitizer.sanitize(name)
    }

  private def desanitizeEntityName(sanitizedEntityName: String): String =
    sanitizedEntityName match {
      case ConfigEntityName.Default => null
      case name => Sanitizer.desanitize(name)
    }

  private def entityToSanitizedUserClientId(entity: ClientQuotaEntity): (Option[String], Option[String]) = {
    if (entity.entries.isEmpty)
      throw new InvalidRequestException("Invalid empty client quota entity")

    var user: Option[String] = None
    var clientId: Option[String] = None
    entity.entries.forEach { (entityType, entityName) =>
      val sanitizedEntityName = Some(sanitizeEntityName(entityName))
      entityType match {
        case ClientQuotaEntity.USER => user = sanitizedEntityName
        case ClientQuotaEntity.CLIENT_ID => clientId = sanitizedEntityName
        case _ => throw new InvalidRequestException(s"Unhandled client quota entity type: ${entityType}")
      }
      if (entityName != null && entityName.isEmpty)
        throw new InvalidRequestException(s"Empty ${entityType} not supported")
    }
    (user, clientId)
  }

  private def userClientIdToEntity(user: Option[String], clientId: Option[String]): ClientQuotaEntity = {
    new ClientQuotaEntity((user.map(u => ClientQuotaEntity.USER -> u) ++ clientId.map(c => ClientQuotaEntity.CLIENT_ID -> c)).toMap.asJava)
  }

  def describeClientQuotas(filter: ClientQuotaFilter): Map[ClientQuotaEntity, Map[String, Double]] = {
    var userComponent: Option[ClientQuotaFilterComponent] = None
    var clientIdComponent: Option[ClientQuotaFilterComponent] = None
    filter.components.forEach { component =>
      component.entityType match {
        case ClientQuotaEntity.USER =>
          if (userComponent.isDefined)
            throw new InvalidRequestException(s"Duplicate user filter component entity type");
          userComponent = Some(component)
        case ClientQuotaEntity.CLIENT_ID =>
          if (clientIdComponent.isDefined)
            throw new InvalidRequestException(s"Duplicate client filter component entity type");
          clientIdComponent = Some(component)
        case "" =>
          throw new InvalidRequestException(s"Unexpected empty filter component entity type")
        case et =>
          // Supplying other entity types is not yet supported.
          throw new UnsupportedVersionException(s"Custom entity type '${et}' not supported")
      }
    }
    handleDescribeClientQuotas(userComponent, clientIdComponent, filter.strict)
  }

  def handleDescribeClientQuotas(userComponent: Option[ClientQuotaFilterComponent],
    clientIdComponent: Option[ClientQuotaFilterComponent], strict: Boolean): Map[ClientQuotaEntity, Map[String, Double]] = {

    def toOption(opt: java.util.Optional[String]): Option[String] =
      if (opt == null)
        None
      else if (opt.isPresent)
        Some(opt.get)
      else
        Some(null)

    val user = userComponent.flatMap(c => toOption(c.`match`))
    val clientId = clientIdComponent.flatMap(c => toOption(c.`match`))

    def sanitized(name: Option[String]): String = name.map(n => sanitizeEntityName(n)).getOrElse("")
    val sanitizedUser = sanitized(user)
    val sanitizedClientId = sanitized(clientId)

    def wantExact(component: Option[ClientQuotaFilterComponent]): Boolean = component.exists(_.`match` != null)
    val exactUser = wantExact(userComponent)
    val exactClientId = wantExact(clientIdComponent)

    def wantExcluded(component: Option[ClientQuotaFilterComponent]): Boolean = strict && !component.isDefined
    val excludeUser = wantExcluded(userComponent)
    val excludeClientId = wantExcluded(clientIdComponent)

    val userEntries = if (exactUser && excludeClientId)
      Map((Some(user.get), None) -> adminZkClient.fetchEntityConfig(ConfigType.User, sanitizedUser))
    else if (!excludeUser && !exactClientId)
      adminZkClient.fetchAllEntityConfigs(ConfigType.User).map { case (name, props) =>
        (Some(desanitizeEntityName(name)), None) -> props
      }
    else
      Map.empty

    val clientIdEntries = if (excludeUser && exactClientId)
      Map((None, Some(clientId.get)) -> adminZkClient.fetchEntityConfig(ConfigType.Client, sanitizedClientId))
    else if (!exactUser && !excludeClientId)
      adminZkClient.fetchAllEntityConfigs(ConfigType.Client).map { case (name, props) =>
        (None, Some(desanitizeEntityName(name))) -> props
      }
    else
      Map.empty

    val bothEntries = if (exactUser && exactClientId)
      Map((Some(user.get), Some(clientId.get)) ->
        adminZkClient.fetchEntityConfig(ConfigType.User, s"${sanitizedUser}/clients/${sanitizedClientId}"))
    else if (!excludeUser && !excludeClientId)
      adminZkClient.fetchAllChildEntityConfigs(ConfigType.User, ConfigType.Client).map { case (name, props) =>
        val components = name.split("/")
        if (components.size != 3 || components(1) != "clients")
          throw new IllegalArgumentException(s"Unexpected config path: ${name}")
        (Some(desanitizeEntityName(components(0))), Some(desanitizeEntityName(components(2)))) -> props
      }
    else
      Map.empty

    def matches(nameComponent: Option[ClientQuotaFilterComponent], name: Option[String]): Boolean = nameComponent match {
      case Some(component) =>
        toOption(component.`match`) match {
          case Some(n) => name.exists(_ == n)
          case None => name.isDefined
        }
      case None =>
        !name.isDefined || !strict
    }

    def fromProps(props: Map[String, String]): Map[String, Double] = {
      props.map { case (key, value) =>
        val doubleValue = try value.toDouble catch {
          case _: NumberFormatException =>
            throw new IllegalStateException(s"Unexpected client quota configuration value: $key -> $value")
        }
        key -> doubleValue
      }
    }

    (userEntries ++ clientIdEntries ++ bothEntries).map { case ((u, c), p) =>
      val quotaProps = p.asScala.filter { case (key, _) => QuotaConfigs.isQuotaConfig(key) }
      if (quotaProps.nonEmpty && matches(userComponent, u) && matches(clientIdComponent, c))
        Some(userClientIdToEntity(u, c) -> fromProps(quotaProps))
      else
        None
    }.flatten.toMap
  }

  def alterClientQuotas(entries: Seq[ClientQuotaAlteration], validateOnly: Boolean): Map[ClientQuotaEntity, ApiError] = {
    def alterEntityQuotas(entity: ClientQuotaEntity, ops: Iterable[ClientQuotaAlteration.Op]): Unit = {
      val (path, configType, configKeys) = entityToSanitizedUserClientId(entity) match {
        case (Some(user), Some(clientId)) => (user + "/clients/" + clientId, ConfigType.User, DynamicConfig.User.configKeys)
        case (Some(user), None) => (user, ConfigType.User, DynamicConfig.User.configKeys)
        case (None, Some(clientId)) => (clientId, ConfigType.Client, DynamicConfig.Client.configKeys)
        case _ => throw new InvalidRequestException("Invalid empty client quota entity")
      }

      val props = adminZkClient.fetchEntityConfig(configType, path)
      ops.foreach { op =>
        op.value match {
          case null =>
            props.remove(op.key)
          case value => configKeys.get(op.key) match {
            case null =>
              throw new InvalidRequestException(s"Invalid configuration key ${op.key}")
            case key => key.`type` match {
              case ConfigDef.Type.DOUBLE =>
                props.setProperty(op.key, value.toString)
              case ConfigDef.Type.LONG =>
                val epsilon = 1e-6
                val longValue = (value + epsilon).toLong
                if ((longValue.toDouble - value).abs > epsilon)
                  throw new InvalidRequestException(s"Configuration ${op.key} must be a Long value")
                props.setProperty(op.key, longValue.toString)
              case _ =>
                throw new IllegalStateException(s"Unexpected config type ${key.`type`}")
            }
          }
        }
      }
      if (!validateOnly)
        adminZkClient.changeConfigs(configType, path, props)
    }
    entries.map { entry =>
      val apiError = try {
        alterEntityQuotas(entry.entity, entry.ops.asScala)
        ApiError.NONE
      } catch {
        case e: Throwable =>
          info(s"Error encountered while updating client quotas", e)
          ApiError.fromThrowable(e)
      }
      entry.entity -> apiError
    }.toMap
  }

  def describeUserScramCredentials(users: Seq[String]): DescribeUserScramCredentialsResponseData = {
    val retval = new DescribeUserScramCredentialsResponseData()

    def addToResults(user: String, userConfig: Properties) = {
      val configKeys = userConfig.stringPropertyNames
      val hasScramCredential = !ScramMechanism.values().toList.filter(key => key != ScramMechanism.UNKNOWN && configKeys.contains(key.toMechanismName)).isEmpty
      if (hasScramCredential) {
        val userScramCredentials = new UserScramCredential().setName(user)
        ScramMechanism.values().foreach(mechanism => if (mechanism != ScramMechanism.UNKNOWN) {
          val propertyValue = userConfig.getProperty(mechanism.toMechanismName)
          if (propertyValue != null) {
            val iterations = ScramCredentialUtils.credentialFromString(propertyValue).iterations()
            userScramCredentials.credentialInfos.add(new CredentialInfo().setMechanism(mechanism.ordinal().toByte).setIterations(iterations))
          }
        })
        retval.userScramCredentials.add(userScramCredentials)
      }
    }

    if (users.isEmpty)
      // describe all users
      adminZkClient.fetchAllEntityConfigs(ConfigType.User).foreach { case (user, properties) => addToResults(user, properties) }
    else {
      // describe specific users
      // https://stackoverflow.com/questions/24729544/how-to-find-duplicates-in-a-list
      val duplicatedUsers = users.groupBy(identity).collect { case (x, Seq(_, _, _*)) => x }
      if (duplicatedUsers.nonEmpty) {
        retval.setError(Errors.INVALID_REQUEST.code())
        retval.setErrorMessage(s"Cannot describe SCRAM credentials for the same user twice in a single request: ${duplicatedUsers.mkString("[", ", ", "]")}")
      } else
        users.foreach { user => addToResults(user, adminZkClient.fetchEntityConfig(ConfigType.User, Sanitizer.sanitize(user))) }
    }
    retval
  }

  def alterUserScramCredentials(upsertions: Seq[AlterUserScramCredentialsRequestData.ScramCredentialUpsertion],
                                deletions: Seq[AlterUserScramCredentialsRequestData.ScramCredentialDeletion]): AlterUserScramCredentialsResponseData = {

    val userMechanismPairs = upsertions.map(upsertion => (upsertion.name, upsertion.mechanism)) ++
      deletions.map(deletion => (deletion.name, deletion.mechanism))

    def errors(error: Errors, errorMessage: String) : AlterUserScramCredentialsResponseData = {
      val retval = new AlterUserScramCredentialsResponseData()
      userMechanismPairs.map(_._1).toSet[String].foreach(user => {
        retval.results.add(new AlterUserScramCredentialsResult().setUser(user).setErrorCode(error.code).setErrorMessage(errorMessage))
      })
      retval
    }

    // https://stackoverflow.com/questions/24729544/how-to-find-duplicates-in-a-list
    val duplicatedUserCredentials = userMechanismPairs.groupBy(p => s"${p._1}:${ScramMechanism.from(p._2).toMechanismName}").collect { case (x, Seq(_, _, _*)) => x }
    if (duplicatedUserCredentials.nonEmpty)
      return errors(Errors.INVALID_REQUEST,s"Cannot alter the same user SCRAM credential twice in a single request: ${duplicatedUserCredentials.mkString("[", ", ", "]")}")
    // check for deletion of a credential that does not exist
    val invalidDeletion = deletions.find(deletion =>
      adminZkClient.fetchEntityConfig(ConfigType.User, Sanitizer.sanitize(deletion.name))
        .getProperty(ScramMechanism.from(deletion.mechanism).toMechanismName) == null)
    if (invalidDeletion.isDefined)
      return errors(Errors.RESOURCE_NOT_FOUND,
        s"Cannot delete SCRAM credential for user ${invalidDeletion.get.name}: no credential for mechanism ${ScramMechanism.from(invalidDeletion.get.mechanism).toMechanismName}")
    // the request is valid
    // TODO: implement-me
    new AlterUserScramCredentialsResponseData()
  }
}
