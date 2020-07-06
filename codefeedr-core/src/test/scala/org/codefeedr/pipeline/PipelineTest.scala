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
 */
package org.codefeedr.pipeline

import java.util

import com.github.sebruck.EmbeddedRedis
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.codefeedr.buffer.{Buffer, BufferType, KafkaBuffer, KafkaTopic}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}
import org.apache.flink.api.scala._
import org.apache.flink.runtime.client.JobExecutionException
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.codefeedr.buffer.serialization.Serializer
import org.codefeedr.stages.{InputStage, OutputStage2}
import org.codefeedr.stages.utilities.{JsonPrinterOutput, StringInput, StringType}
import org.codefeedr.testUtils._
import org.codefeedr.util.schema_exposure.{RedisSchemaExposer, ZookeeperSchemaExposer}
import redis.embedded.RedisServer

import scala.collection.JavaConverters._

class StageOne(name: String, input: Seq[String])
    extends InputStage[StringType](Some(name)) {
  override def main(context: Context): DataStream[StringType] = {
    context.env
      .fromCollection(input.map(StringType(_)))
  }
}

class StageTwo(elementsExpected: Int)
    extends OutputStage2[StringType, StringType] {
  override def main(source: DataStream[StringType],
                    secondSource: DataStream[StringType]): Unit = {
    source.addSink(new StringTypeSink(elementsExpected))
  }
}

object StringTypeSink {
  val result = new util.ArrayList[StringType]() //mutable list
}

class StringTypeSink(elementsExpected: Int) extends SinkFunction[StringType] {

  var amount = elementsExpected

  override def invoke(value: StringType): Unit = {
    synchronized {
      StringTypeSink.result.add(value)

      amount = amount - 1

      if (amount <= 0) throw new JobFinishedException()
    }
  }

}

class PipelineTest
    extends FunSuite
    with BeforeAndAfter
    with EmbeddedKafka
    with EmbeddedRedis
    with BeforeAndAfterAll {

  var builder: PipelineBuilder = _

  var redis: RedisServer = null
  var redisPort: Int = 0

  override def beforeAll() = {
    implicit val config =
      EmbeddedKafkaConfig(zooKeeperPort = 2181, kafkaPort = 9092)
    EmbeddedKafka.start()(config)

    redis = startRedis()
    redisPort = redis.ports().get(0)
  }

  override def afterAll(): Unit = {
    EmbeddedKafka.stop()
    stopRedis(redis)
  }

  before {
    builder = new PipelineBuilder()
    CollectSink.result.clear()
  }

  test("Simple pipeline test wordcount") {
    val list = "hallo hallo doei doei doei".split("[ \n]")

    builder
      .appendSource { x: StreamExecutionEnvironment =>
        x.fromCollection(list).map(StringType(_))
      }
      .append { x: DataStream[StringType] =>
        x.map(x => (x.value, 1))
          .keyBy(0)
          .sum(1)
          .map(x => WordCount(x._1, x._2))
      }
      .append { x: DataStream[WordCount] =>
        x.addSink(new CollectSink)
      }
      .build()
      .startMock()

    val res = CollectSink.result.asScala

    assert(res.contains(WordCount("doei", 3)))
    assert(res.contains(WordCount("hallo", 2)))
  }

  test("Simple pipeline test") {
    builder
      .append(new StringInput())
      .append { x: DataStream[StringType] =>
        x.map(x => WordCount(x.value, 1))
          .setParallelism(1)
          .addSink(new CollectSink)
          .setParallelism(1)
      }
      .build()
      .startMock()

    val res = CollectSink.result.asScala

    assert(res.contains(WordCount("", 1)))
  }

  test("Non-sequential pipeline mock test") {
    val pipeline = simpleDAGPipeline().build()

    assertThrows[IllegalStateException] {
      pipeline.start(Array("-runtime", "mock"))
    }
  }

  test("Non-sequential pipeline local test") {
    val pipeline = simpleDAGPipeline(1)
      .setBufferProperty(Buffer.SERIALIZER, Serializer.BSON)
      .setBufferType(BufferType.Kafka)
      .build()

    assertThrows[JobExecutionException] {
      pipeline.startLocal()
    }
  }

  test("Simple pipeline schema exposure test (redis)") {
    val pipeline = simpleDAGPipeline(2)
      .setBufferType(BufferType.Kafka)
      .setBufferProperty(KafkaBuffer.SCHEMA_EXPOSURE, "true")
      .setBufferProperty(KafkaBuffer.SCHEMA_EXPOSURE_HOST,
                         s"redis://localhost:$redisPort")
      .build()

    assertThrows[JobExecutionException] {
      pipeline.start(Array("-runtime", "local"))
    }

    val exposer = new RedisSchemaExposer(s"redis://localhost:$redisPort")

    val schema1 =
      exposer.get("org.codefeedr.testUtils.SimpleSourceStage")
    val schema2 =
      exposer.get("org.codefeedr.testUtils.SimpleTransformStage")

    assert(schema1.nonEmpty)
    assert(schema2.nonEmpty)
  }

  test("Simple pipeline schema exposure and deserialization test (redis)") {
    val pipeline = simpleDAGPipeline(2)
      .setBufferType(BufferType.Kafka)
      .setBufferProperty(KafkaBuffer.SCHEMA_EXPOSURE, "true")
      .setBufferProperty(KafkaBuffer.SCHEMA_EXPOSURE_DESERIALIZATION, "true")
      .setBufferProperty(KafkaBuffer.SCHEMA_EXPOSURE_HOST,
                         s"redis://localhost:$redisPort")
      .build()

    assertThrows[JobExecutionException] {
      pipeline.startLocal()
    }
  }

  test("Simple pipeline schema exposure test (zookeeper)") {
    val pipeline = simpleDAGPipeline(2)
      .setBufferType(BufferType.Kafka)
      .setBufferProperty(KafkaBuffer.SCHEMA_EXPOSURE, "true")
      .setBufferProperty(KafkaBuffer.SCHEMA_EXPOSURE_SERVICE, "zookeeper")
      .setBufferProperty(KafkaBuffer.SCHEMA_EXPOSURE_HOST, "localhost:2181")
      .setBufferProperty(KafkaBuffer.START_POSITION, KafkaBuffer.TIMESTAMP)
      .setBufferProperty(KafkaBuffer.START_TIMESTAMP, "0")
      .build()

    assertThrows[JobExecutionException] {
      pipeline.startLocal()
    }

    val exposer = new ZookeeperSchemaExposer("localhost:2181")

    val schema1 =
      exposer.get("org.codefeedr.testUtils.SimpleSourceStage")
    val schema2 =
      exposer.get("org.codefeedr.testUtils.SimpleTransformStage")

    assert(schema1.nonEmpty)
    assert(schema2.nonEmpty)
  }

  test("A pipeline name can be set through args") {
    val name = "nice pipeline"
    val pipeline = builder
      .append(new SimpleSourceStage())
      .append(new SimpleSinkStage())
      .build()

    pipeline.start(Array[String]("-runtime", "mock", "-name", name))

    assert(pipeline.name == name)
  }

  test("A pipeline has a default name") {
    val pipeline = builder
      .disablePipelineVerification()
      .append(new KafkaTopic[StringType]("fake"))
      .append(new SimpleSourceStage())
      .append(new SimpleSinkStage())
      .build()

    pipeline.startMock()

    assert(pipeline.name == "CodeFeedr pipeline")
  }

  /**
    * Builds a really simple (DAG) graph
    * @param expectedMessages after the job will finish.
    * @return a pipelinebuilder (add elements or finish it with .build())
    */
  def simpleDAGPipeline(expectedMessages: Int = -1) = {
    val source = new SimpleSourceStage()
    val a = new SimpleTransformStage()
    val b = new SimpleTransformStage()
    val sink = new SimpleSinkStage(expectedMessages)

    builder
      .append(source)
      .disablePipelineVerification()
      .setPipelineType(PipelineType.DAG)
      .edge(source, a)
      .edge(source, b)
      .edge(a, sink)
      .edge(b, sink)
  }

  /***************** CLUSTER *********************/
  test("Cluster runtime starts the correct object") {
    val source = new HitObjectTest()
    val sink = new SimpleSinkStage(1)

    val pipeline = builder
      .disablePipelineVerification()
      .setBufferType(BufferType.Kafka)
      .edge(source, sink)
      .build()

    assertThrows[CodeHitException] {
      pipeline.start(
        Array("-runtime",
              "cluster",
              "-stage",
              "org.codefeedr.testUtils.HitObjectTest"))
    }
  }

  test("Should throw when trying to start an unknown cluster object") {
    val source = new HitObjectTest()
    val sink = new SimpleSinkStage(1)

    val pipeline = builder
      .disablePipelineVerification()
      .setBufferType(BufferType.Kafka)
      .edge(source, sink)
      .build()

    assertThrows[StageNotFoundException] {
      pipeline.startClustered("org.codefeedr.testUtils.DOesNotExtst")
    }
  }

  test("Should execute flink code when starting cluster object") {
    val source = new FlinkCrashObjectTest()
    val sink = new SimpleSinkStage(1)

    val pipeline = builder
      .disablePipelineVerification()
      .setBufferType(BufferType.Kafka)
      .edge(source, sink)
      .build()

    assertThrows[JobExecutionException] {
      pipeline.startClustered("org.codefeedr.testUtils.FlinkCrashObjectTest")
    }
  }

  test("Should throw when propertiesOf stage is null") {
    val sink = new SimpleSinkStage(1)
    val source = new SimpleSourceStage()

    val pipeline = builder
      .edge(source, sink)
      .build()

    assertThrows[IllegalArgumentException] {
      pipeline.propertiesOf(null)
    }
  }

  test("Show list of pipeline item ids while fake topic is ignored.") {
    val pipeline = builder
      .disablePipelineVerification()
      .append(new StringInput())
      .append(new JsonPrinterOutput())
      .append(new KafkaTopic("kafka_name"))
      .build()

    val stream = new java.io.ByteArrayOutputStream()
    Console.withOut(stream) {
      pipeline.start(Array("--list"))
    }

    val output = stream.toString

    assert(output.split("\n").length == 2)
    assert(output.startsWith("string_input"))
  }

  test("Show list of pipeline item ids in an exception") {
    val pipeline = builder
      .disablePipelineVerification()
      .append(new StringInput())
      .append(new JsonPrinterOutput())
      .build()

    assertThrows[PipelineListException] {
      pipeline.start(Array("--list", "--asException"))
    }
  }

  test("Order of pipeline is based on addition order of edges.") {
    val stageOne = new StageOne("firstStage", Seq("a", "b", "c"))
    val stageTwo = new StageOne("secondStage", Seq("d", "e"))
    val finalStage = new StageTwo(3)

    val pipeline = builder
      .edge(stageOne, finalStage)
      .edge(stageTwo, finalStage)
      .build()

    assertThrows[JobExecutionException] {
      pipeline.startLocal()
    }

    assert(StringTypeSink.result.size == 3)
    assert(
      StringTypeSink.result.asScala.map(_.value).toSet == Set("a", "b", "c"))

  }
}
