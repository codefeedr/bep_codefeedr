/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.codefeedr.buffer

import java.util.{Optional, Properties}

import org.apache.avro.reflect.ReflectData
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner
import org.apache.flink.streaming.connectors.kafka.{
  FlinkKafkaConsumer,
  FlinkKafkaProducer
}
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.logging.log4j.scala.Logging
import org.codefeedr.buffer.serialization.schema_exposure.{
  RedisSchemaExposer,
  SchemaExposer,
  ZookeeperSchemaExposer
}
import org.codefeedr.pipeline.Pipeline

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import org.codefeedr.Properties._

/** Holds Kafka property names. */
object KafkaBuffer {

  ///KAFKA RELATED
  val BROKER = "bootstrap.servers"
  val ZOOKEEPER = "zookeeper.connect"

  //SCHEMA EXPOSURE
  val SCHEMA_EXPOSURE = "SCHEMA_EXPOSURE"
  val SCHEMA_EXPOSURE_SERVICE = "SCHEMA_EXPOSURE_SERVICE"
  val SCHEMA_EXPOSURE_HOST = "SCHEMA_EXPOSURE_HOST"
  val SCHEMA_EXPOSURE_DESERIALIZATION = "SCHEMA_EXPOSURE_SERIALIZATION"

  //PARTITIONS, REPLICAS AND COMPRESSION
  val AMOUNT_OF_PARTITIONS = "AMOUNT_OF_PARTITONS"
  val AMOUNT_OF_REPLICAS = "AMOUNT_OF_REPLICAS"
  val COMPRESSION_TYPE = "compression.type"

  //OFFSETS
  val START_POSITION = "START_POSITION"
  val START_TIMESTAMP = "START_TIMESTAMP"
  val GROUP_OFFSETS = "GROUP_OFFSETS"
  val TIMESTAMP = "TIMESTAMP"
  val LATEST = "LATEST"
  val EARLIEST = "EARLIEST"

  //SEMANTICS
  val SEMANTIC = "SEMANTIC"

}

/** The implementation for the Kafka buffer. This buffer is the default.
  *
  * @param pipeline The pipeline for which we use this Buffer.
  * @param properties The properties of this Buffer.
  * @param topic The topic to write to, which is basically the subject.
  * @param groupId The group id, to specify the consumer group in Kafka.
  * @tparam T Type of the data in this Buffer.
  */
class KafkaBuffer[T <: Serializable with AnyRef: ClassTag: TypeTag](
  pipeline: Pipeline,
  properties: org.codefeedr.Properties,
  topic: String,
  groupId: String
) extends Buffer[T](pipeline, properties, topic)
    with Logging {

  implicit def stringToSemantic(str: String): Semantic = Semantic.valueOf(str)

  /** Default settings for the Kafka buffer. */
  private object KafkaBufferDefaults {
    //KAFKA RELATED
    val BROKER = "localhost:9092"
    val ZOOKEEPER = "localhost:2181"

    val AUTO_OFFSET_RESET = "earliest"
    val AUTO_COMMIT_INTERVAL_MS = "100"
    val ENABLE_AUTO_COMMIT = "true"

    //SCHEMA EXPOSURE
    val SCHEMA_EXPOSURE = false
    val SCHEMA_EXPOSURE_SERVICE = "redis"
    val SCHEMA_EXPOSURE_HOST = "redis://localhost:6379"

    //PARTITIONS, REPLICAS AND COMPRESSION
    val AMOUNT_OF_PARTITIONS = 1
    val AMOUNT_OF_REPLICAS = 1
    val COMPRESSION_TYPE = "none"

    //OFFSETS
    val START_POSITION = KafkaBuffer.GROUP_OFFSETS
    val START_TIMESTAMP = 0x0

    //SEMANTIC
    val SEMANTIC: Semantic = Semantic.AT_LEAST_ONCE
  }

  /** Get a Kafka Consumer as source for a stage.
    *
    * @return The DataStream retrieved from the buffer.
    */
  override def getSource: DataStream[T] = {
    val serde = getSerializer()

    // Make sure the topic already exists, otherwise create it.
    checkAndCreateSubject(
      topic,
      properties
        .get[String](KafkaBuffer.BROKER)
        .getOrElse(KafkaBufferDefaults.BROKER)
    )

    val kafkaConsumer =
      new FlinkKafkaConsumer[T](topic, serde, getKafkaProperties)
    val startPosition = properties.getOrElse[String](
      KafkaBuffer.START_POSITION,
      KafkaBufferDefaults.START_POSITION
    )

    /** Configure the starting point FromGroupOffsets. */
    startPosition match {
      case KafkaBuffer.EARLIEST => kafkaConsumer.setStartFromEarliest()
      case KafkaBuffer.LATEST   => kafkaConsumer.setStartFromLatest()
      case KafkaBuffer.TIMESTAMP =>
        kafkaConsumer.setStartFromTimestamp(
          properties.getOrElse[Long](
            KafkaBuffer.START_TIMESTAMP,
            KafkaBufferDefaults.START_TIMESTAMP
          )(_.toLong)
        )
      case KafkaBuffer.GROUP_OFFSETS => kafkaConsumer.setStartFromGroupOffsets()
      case _                         => kafkaConsumer.setStartFromGroupOffsets()
    }

    // Add a source.
    pipeline.environment
      .addSource(kafkaConsumer)
      .name(s"Consuming from topic /$topic")
  }

  /** Get a Kafka Producer as sink to the buffer.
    *
    * @return The SinkFunction created by this Producer.
    */
  override def getSink: SinkFunction[T] = {
    // Check if a schema should be exposed.
    if (properties
          .get[Boolean](KafkaBuffer.SCHEMA_EXPOSURE)
          .getOrElse(KafkaBufferDefaults.SCHEMA_EXPOSURE)) {

      exposeSchema()
    }

    // Make sure the topic already exists, otherwise create it.
    checkAndCreateSubject(
      topic,
      properties
        .getOrElse[String](KafkaBuffer.BROKER, KafkaBufferDefaults.BROKER)
    )

    // Check preferred partitioning
    val semantic = properties.getOrElse[Semantic](
      KafkaBuffer.SEMANTIC,
      KafkaBufferDefaults.SEMANTIC
    )(stringToSemantic)

    // Create Kafka producer.
    val producer =
      new FlinkKafkaProducer[T](
        topic,
        getSerializer(topic),
        getKafkaProperties,
        semantic
      )
    producer.setWriteTimestampToKafka(true)

    producer
  }

  /** Get all the Kafka properties.
    *
    * @return A map with all the properties.
    */
  def getKafkaProperties: java.util.Properties = {
    val kafkaProp = new java.util.Properties()
    kafkaProp.put("bootstrap.servers", KafkaBufferDefaults.BROKER)
    kafkaProp.put("zookeeper.connect", KafkaBufferDefaults.ZOOKEEPER)
    kafkaProp.put("auto.offset.reset", KafkaBufferDefaults.AUTO_OFFSET_RESET)
    kafkaProp.put(
      "auto.commit.interval.ms",
      KafkaBufferDefaults.AUTO_COMMIT_INTERVAL_MS
    )
    kafkaProp.put("enable.auto.commit", KafkaBufferDefaults.ENABLE_AUTO_COMMIT)
    kafkaProp.put("compression.type", KafkaBufferDefaults.COMPRESSION_TYPE)
    kafkaProp.put("group.id", groupId)

    properties.getContents.foreach {
      case (k, v) =>
        kafkaProp.setProperty(k, v)
    }

    kafkaProp
  }

  /** Checks if a Kafka topic exists, if not it is created.
    *
    * @param topic      The topic to create.
    * @param connection The kafka broker to connect to.
    */
  def checkAndCreateSubject(topic: String, connection: String): Unit = {
    // Set all the correct properties.
    val props = new Properties()
    props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, connection)

    // Connect with Kafka.
    val adminClient = AdminClient.create(props)

    // Check if topic already exists.
    val alreadyCreated = doesTopicExist(adminClient, topic)

    // If topic doesnt exist yet, create it.
    if (!alreadyCreated) {
      // The topic configuration will probably be overwritten by the producer.
      logger.info(s"Topic $topic doesn't exist yet, now creating it.")
      val newTopic = new NewTopic(
        topic,
        properties.getOrElse[Int](
          KafkaBuffer.AMOUNT_OF_PARTITIONS,
          KafkaBufferDefaults.AMOUNT_OF_PARTITIONS
        )(_.toInt),
        properties
          .getOrElse[Int](
            KafkaBuffer.AMOUNT_OF_REPLICAS,
            KafkaBufferDefaults.AMOUNT_OF_REPLICAS
          )(_.toInt)
          .asInstanceOf[Short]
      )
      createTopic(adminClient, newTopic)
    }
  }

  /** Verify a Kafka topic already exists.
    *
    * @param client The connection to the Kafka broker.
    * @param topic The topic to check.
    * @return True if the topic already exists.
    */
  private def doesTopicExist(client: AdminClient, topic: String): Boolean = {
    client
      .listTopics()
      .names()
      .get()
      .contains(topic)
  }

  /** Creates a new topic.
    *
    * @param client The connection to the Kafka broker.
    * @param topic The topic to create.
    */
  private def createTopic(client: AdminClient, topic: NewTopic): Unit = {
    client
      .createTopics(List(topic).asJavaCollection)
      .all()
      .get() // This blocks the method until the topic is created.
  }

  /** Exposes the Avro schema to an external service (like redis/zookeeper).
    *
    * @return True if successfully exposed.
    */
  def exposeSchema(): Boolean = {
    //get the schema
    val schema = ReflectData.get().getSchema(inputClassType)

    //expose the schema
    getExposer.put(schema, topic)
  }

  /** Get schema exposer based on configuration.
    *
    * @return A schema exposer instance.
    */
  def getExposer: SchemaExposer = {
    val exposeName = properties
      .get[String](KafkaBuffer.SCHEMA_EXPOSURE_SERVICE)
      .getOrElse(KafkaBufferDefaults.SCHEMA_EXPOSURE_SERVICE)

    val exposeHost = properties
      .get[String](KafkaBuffer.SCHEMA_EXPOSURE_HOST)
      .getOrElse(KafkaBufferDefaults.SCHEMA_EXPOSURE_HOST)

    // Get the exposer.
    val exposer = exposeName match {
      case "zookeeper" => new ZookeeperSchemaExposer(exposeHost)
      case _           => new RedisSchemaExposer(exposeHost) //default is redis
    }

    exposer
  }
}
