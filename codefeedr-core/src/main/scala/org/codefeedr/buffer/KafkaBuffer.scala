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

import java.util.Properties

import org.apache.avro.Schema
import org.apache.avro.reflect.ReflectData
import org.codefeedr.Properties._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

import scala.reflect.{ClassTag, classTag}
import scala.reflect.runtime.universe._
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.codefeedr.buffer.serialization.schema_exposure.{RedisSchemaExposer, SchemaExposer, ZookeeperSchemaExposer}
import org.codefeedr.pipeline.Pipeline
import org.codefeedr.stages.StageAttributes

import scala.collection.JavaConverters._

object KafkaBuffer {
  /**
    * PROPERTIES
    */
  val BROKER = "HOST"
  val ZOOKEEPER = "ZOOKEEPER"

  //SCHEMA EXPOSURE
  val SCHEMA_EXPOSURE = "SCHEMA_EXPOSURE"
  val SCHEMA_EXPOSURE_SERVICE = "SCHEMA_EXPOSURE_SERVICE"
  val SCHEMA_EXPOSURE_HOST = "SCHEMA_EXPOSURE_HOST"
  val SCHEMA_EXPOSURE_DESERIALIZATION = "SCHEMA_EXPOSURE_SERIALIZATION"
}

class KafkaBuffer[T <: AnyRef : ClassTag : TypeTag](pipeline: Pipeline, properties: org.codefeedr.Properties, stageAttributes: StageAttributes, topic: String, groupId: String)
  extends Buffer[T](pipeline, properties) {

  private object KafkaBufferDefaults {
    /**
      * DEFAULT VALUES
      */
    val BROKER = "localhost:9092"
    val ZOOKEEPER = "localhost:2181"

    //SCHEMA EXPOSURE
    val SCHEMA_EXPOSURE = false
    val SCHEMA_EXPOSURE_SERVICE = "redis"
    val SCHEMA_EXPOSURE_HOST = "redis://localhost:6379"
  }

  override def getSource: DataStream[T] = {
    val serde = getSerializer

    //make sure the topic already exists
    checkAndCreateSubject(topic, properties.get[String](KafkaBuffer.BROKER).
      getOrElse(KafkaBufferDefaults.BROKER))

    pipeline.environment.
      addSource(new FlinkKafkaConsumer011[T](topic, serde, getKafkaProperties))
  }

  override def getSink: SinkFunction[T] = {
    //check if a schema should be exposed
    if (properties.get[Boolean](KafkaBuffer.SCHEMA_EXPOSURE)
      .getOrElse(KafkaBufferDefaults.SCHEMA_EXPOSURE)) {

      exposeSchema()
    }

    val producer = new FlinkKafkaProducer011[T](topic, getSerializer, getKafkaProperties)
    producer.setWriteTimestampToKafka(true)

    producer
  }

  /**
    * Get all the kafka properties.
    *
    * @return a map with all the properties.
    */
  def getKafkaProperties: java.util.Properties = {
    val kafkaProp = new java.util.Properties()
    kafkaProp.put("bootstrap.servers", properties.getOrElse[String](KafkaBuffer.BROKER, KafkaBufferDefaults.BROKER))
    kafkaProp.put("zookeeper.connect", properties.getOrElse[String](KafkaBuffer.ZOOKEEPER, KafkaBufferDefaults.ZOOKEEPER))
    kafkaProp.put("auto.offset.reset", "earliest")
    kafkaProp.put("auto.commit.interval.ms", "100")
    kafkaProp.put("enable.auto.commit", "true")
    kafkaProp.put("group.id", groupId)

    kafkaProp
  }

  /**
    * Checks if a Kafka topic exists, if not it is created.
    *
    * @param topic      the topic to create.
    * @param connection the kafka broker to connect to.
    */
  def checkAndCreateSubject(topic: String, connection: String): Unit = {
    //set all the correct properties
    val props = new Properties()
    props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, connection)

    //connect with Kafka
    val adminClient = AdminClient.create(props)

    //check if topic already exists
    val alreadyCreated = adminClient
      .listTopics()
      .names()
      .get()
      .contains(topic)

    //if topic doesnt exist yet, create it
    if (!alreadyCreated) {
      //the topic configuration will probably be overwritten by the producer
      //TODO check this ^
      println(s"Topic $topic doesn't exist yet, now creating it.")
      val newTopic = new NewTopic(topic, 1, 1)
      adminClient.
        createTopics(List(newTopic).asJavaCollection)
        .all()
        .get() //this blocks the method until the topic is created
    }
  }

  /**
    * Exposes the Avro schema to an external service (like redis/zookeeper).
    */
  def exposeSchema(): Boolean = {
    //get the schema
    val schema = ReflectData.get().getSchema(inputClassType)

    //expose the schema
    getExposer.put(schema, topic)
  }

  /**
    * Get schema exposer based on configuration.
    *
    * @return a schema exposer.
    */
  def getExposer: SchemaExposer = {
    val exposeName = properties.
      get[String](KafkaBuffer.SCHEMA_EXPOSURE_SERVICE).
      getOrElse(KafkaBufferDefaults.SCHEMA_EXPOSURE_SERVICE)

    val exposeHost = properties.
      get[String](KafkaBuffer.SCHEMA_EXPOSURE_HOST).
      getOrElse(KafkaBufferDefaults.SCHEMA_EXPOSURE_HOST)

    //get exposer
    val exposer = exposeName match {
      case "zookeeper" => new ZookeeperSchemaExposer(exposeHost)
      case _ => new RedisSchemaExposer(exposeHost) //default is redis
    }

    exposer
  }
}

