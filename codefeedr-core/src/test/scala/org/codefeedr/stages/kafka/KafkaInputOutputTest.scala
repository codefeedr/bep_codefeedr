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
package org.codefeedr.stages.kafka

import java.util
import java.util.{Optional, Properties, UUID}

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.flink.runtime.client.JobExecutionException
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction.Context
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.codefeedr.pipeline.PipelineBuilder
import org.codefeedr.stages.OutputStage
import org.codefeedr.stages.utilities.{StringInput, StringType}
import org.codefeedr.testUtils.JobFinishedException
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.collection.JavaConverters._

class KafkaInputOutputTest
    extends FunSuite
    with EmbeddedKafka
    with BeforeAndAfterAll {

  val someInput = "hi\nthis\nis\na\nreally\nnice\ntest"

  override def beforeAll(): Unit = {
    implicit val config =
      EmbeddedKafkaConfig(zooKeeperPort = 2181, kafkaPort = 9092)
    EmbeddedKafka.start()
  }

  override def afterAll(): Unit = {
    EmbeddedKafka.stop()
  }

  test("Data should be properly sent and read") {
    val properties = new Properties()
    properties.put("bootstrap.servers", "localhost:9092")
    properties.put("enable.auto.commit", "true")
    properties.put("auto.commit.interval.ms", "10")

    val topic = UUID.randomUUID().toString
    checkAndCreateSubject(topic, "localhost:9092")

    val pipeline =
      new PipelineBuilder()
        .edge(new StringInput(someInput),
              new KafkaOutput[StringType](topic, properties))
        .edge(new KafkaInput[StringType](topic, properties),
              new KafkaStringOutput(7))
        .build()

    assertThrows[JobExecutionException] {
      pipeline.startLocal()
      assert(KafkaStringCollectSink.result.size() == 7)

    }



  }

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
      val newTopic = new NewTopic(topic, 1, 1.toShort)
      adminClient
        .createTopics(List(newTopic).asJavaCollection)
        .all()
        .get() //this blocks the method until the topic is created
    }

  }

}

object KafkaStringCollectSink {
  var result = new util.ArrayList[String]() //mutable list

  def asList: List[String] = result.asScala.toList
}

class KafkaStringCollectSink(amount: Int) extends SinkFunction[StringType] {

  var amountLeft = amount

  override def invoke(value: StringType, context: Context[_]): Unit = {
    synchronized {
      KafkaStringCollectSink.result.add(value.value)

      amountLeft = amountLeft - 1
      println(s"Added new element ${value.value}, amountLeft: $amountLeft")
      if (amountLeft == 0) {
        throw new JobFinishedException()
      }

    }
  }

}

class KafkaStringOutput(amount: Int) extends OutputStage[StringType] {
  override def main(source: DataStream[StringType]): Unit =
    source.addSink(new KafkaStringCollectSink(amount)).setParallelism(1)
}
