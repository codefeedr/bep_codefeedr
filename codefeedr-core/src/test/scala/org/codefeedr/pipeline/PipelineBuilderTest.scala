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

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.restartstrategy.RestartStrategies.RestartStrategyConfiguration
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.{
  DataStream,
  StreamExecutionEnvironment
}
import org.codefeedr.keymanager.StaticKeyManager
import org.codefeedr.buffer.{Buffer, BufferType}
import org.apache.flink.api.scala._
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.codefeedr.buffer.serialization.Serializer
import org.codefeedr.stages.utilities.StringType
import org.codefeedr.stages.OutputStage
import org.codefeedr.testUtils.{
  SimpleSinkStage,
  SimpleSourceStage,
  SimpleTransformStage,
  SimpleTwoTransformStage
}
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

class PipelineBuilderTest extends FunSuite with BeforeAndAfter with Matchers {

  var builder: PipelineBuilder = _

  before {
    builder = new PipelineBuilder()
  }

  test("An Simple pipeline configuration throws") {
    assertThrows[EmptyPipelineException] {
      val pipeline = builder.build()
    }
  }

  test("Set buffertype should also be retrievable") {
    builder.setBufferType(BufferType.Kafka)

    assert(builder.getBufferType == BufferType.Kafka)
  }

  test("Every pipeline object should appear in the pipeline (1)") {
    val pipeline = builder
      .append(new SimpleSourceStage())
      .disablePipelineVerification()
      .build()

    assert(pipeline.graph.nodes.size == 1)

    pipeline.graph.nodes.head shouldBe an[SimpleSourceStage]
  }

  test("Every pipeline object should appear in the pipeline (2)") {
    val pipeline = builder
      .append(new SimpleSourceStage())
      .append(new SimpleTransformStage())
      .build()

    assert(pipeline.graph.nodes.size == 2)

    pipeline.graph.nodes.head shouldBe an[SimpleSourceStage]
    pipeline.graph.nodes.last shouldBe an[SimpleTransformStage]
  }

  test("Set properties should be available in stage properties") {
    val stage = new SimpleSourceStage()
    val stage2 = new SimpleTransformStage()

    val pipeline = builder
      .append(stage)
      .disablePipelineVerification()
      .setStageProperty(stage, "key", "value")
      .setStageProperty(stage, "anotherKey", "true")
      .build()

    assert(pipeline.propertiesOf(stage).get("key").get == "value")
    assert(pipeline.propertiesOf(stage2).get("key").isEmpty)
  }

  test(
    "Set buffer properties should be available in pipeline buffer properties") {
    val stage = new SimpleSourceStage()
    val pipeline = builder
      .append(stage)
      .setBufferProperty("key", "value")
      .build()

    assert(pipeline.bufferProperties.get("key").get == "value")
  }

  test("A set keymanager should be forwarded to the pipeline") {
    val km = new StaticKeyManager()

    val pipeline = builder
      .append(new SimpleSourceStage())
      .setKeyManager(km)
      .build()

    assert(pipeline.keyManager == km)
  }

  test("A pipeline name should be set") {
    val name = "Simple pipeline"

    val pipeline = builder
      .append(new SimpleSourceStage())
      .setPipelineName(name)
      .build()

    assert(pipeline.name == name)
  }

  test("A DAG pipeline can't be appeneded to") {
    builder.edge(new SimpleSourceStage(), new SimpleTransformStage())

    assertThrows[IllegalStateException] {
      builder.append(new SimpleSourceStage())
    }
  }

  test("A sequential pipeline cannot switch to a DAG automatically") {
    builder.append(new SimpleSourceStage())

    assertThrows[IllegalStateException] {
      builder.edge(new SimpleTransformStage(), new SimpleSinkStage())
    }
  }

  test("A sequential pipeline can switch to a DAG manually") {
    builder.append(new SimpleSourceStage())
    builder.setPipelineType(PipelineType.DAG)

    builder.edge(new SimpleTransformStage(), new SimpleSinkStage())
  }

  test("Default TimeCharacteristic is EventTime") {
    val pipeline = builder.append(new SimpleSourceStage()).build()

    assert(
      pipeline.pipelineProperties.streamTimeCharacteristic == TimeCharacteristic.EventTime)
  }

  test("TimeCharacteristic can be overridden.") {
    val pipeline = builder
      .append(new SimpleSourceStage())
      .setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
      .build()

    assert(
      pipeline.pipelineProperties.streamTimeCharacteristic == TimeCharacteristic.IngestionTime)
  }

  test("Default RestartStrategy is no restart.") {
    val pipeline = builder.append(new SimpleSourceStage()).build()

    assert(
      pipeline.pipelineProperties.restartStrategy == RestartStrategies
        .noRestart())
  }

  test("Default RestartStrategy can be overriden.") {
    val pipeline = builder
      .append(new SimpleSourceStage())
      .setRestartStrategy(RestartStrategies.fallBackRestart())
      .build()

    assert(
      pipeline.pipelineProperties.restartStrategy == RestartStrategies
        .fallBackRestart())
  }

  test("Default StateBackend is memory") {
    val pipeline = builder.append(new SimpleSourceStage()).build()

    assert(
      pipeline.pipelineProperties.stateBackend.isInstanceOf[MemoryStateBackend])
  }

  test("Default StateBackend can be overriden.") {
    val pipeline = builder
      .append(new SimpleSourceStage())
      .setStateBackend(new FsStateBackend("file://test/test"))
      .build()

    assert(
      pipeline.pipelineProperties.stateBackend.isInstanceOf[FsStateBackend])
  }

  test("Default checkpointing is disabled") {
    val pipeline = builder.append(new SimpleSourceStage()).build()

    assert(pipeline.pipelineProperties.checkpointing.isEmpty)
    assert(
      pipeline.pipelineProperties.checkpointingMode == CheckpointingMode.EXACTLY_ONCE)
  }

  test("Default checkpointing can be enabled.") {
    val pipeline =
      builder.append(new SimpleSourceStage()).enableCheckpointing(1000).build()

    assert(pipeline.pipelineProperties.checkpointing.get == 1000)
  }

  test("Default checkpointing can be enabled and mode can be set.") {
    val pipeline =
      builder
        .append(new SimpleSourceStage())
        .enableCheckpointing(500, CheckpointingMode.AT_LEAST_ONCE)
        .build()

    assert(pipeline.pipelineProperties.checkpointing.get == 500)
    assert(
      pipeline.pipelineProperties.checkpointingMode == CheckpointingMode.AT_LEAST_ONCE)
  }

  test("Checkpointmode can be overriden.") {
    val pipeline =
      builder
        .append(new SimpleSourceStage())
        .setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)
        .build()

    assert(
      pipeline.pipelineProperties.checkpointingMode == CheckpointingMode.AT_LEAST_ONCE)
    assert(pipeline.pipelineProperties.checkpointing.isEmpty)
  }

  test("A non-sequential pipeline cannot switch to a sequential pipeline") {
    val a = new SimpleSourceStage()
    val b = new SimpleTransformStage()
    val c = new SimpleTransformStage()

    builder.edge(a, b)
    builder.edge(a, c)

    assert(builder.getPipelineType == PipelineType.DAG)

    assertThrows[IllegalStateException] {
      builder.setPipelineType(PipelineType.Sequential)
    }
  }

  test("Can't add edges to the DAG pipeline twice") {
    val a = new SimpleSourceStage()
    val b = new SimpleTransformStage()

    builder.edge(a, b)

    assertThrows[IllegalArgumentException] {
      builder.edge(a, b)
    }
  }

  test("Direct link from one stage to a list of others.") {
    val a = new SimpleSourceStage()
    val b = new SimpleTransformStage()
    val c = new SimpleTransformStage()

    builder.edge(a, List(b, c))

    assert(builder.getPipelineType == PipelineType.DAG)

    val pipeline = builder.build()

    assert(pipeline.graph.getChildren(a).size == 2)
  }

  test("Direct link from list of stages to one stage.") {
    val a = new SimpleSourceStage()
    val b = new SimpleSourceStage()
    val c = new SimpleTwoTransformStage()

    builder.edge(List(a, b), c)

    assert(builder.getPipelineType == PipelineType.DAG)

    val pipeline = builder.build()

    assert(pipeline.graph.getParents(c).size == 2)
  }

  test("Direct link from list of stages to other list of stages.") {
    val a = new SimpleSourceStage()
    val b = new SimpleSourceStage()
    val c = new SimpleTwoTransformStage()
    val d = new SimpleTwoTransformStage()
    val e = new SimpleTwoTransformStage()

    builder.edge(List(a, b), List(c, d, e))

    assert(builder.getPipelineType == PipelineType.DAG)

    val pipeline = builder.build()

    assert(pipeline.graph.getParents(c).size == 2)
    assert(pipeline.graph.getParents(d).size == 2)
    assert(pipeline.graph.getParents(e).size == 2)
    assert(pipeline.graph.getChildren(a).size == 3)
    assert(pipeline.graph.getChildren(b).size == 3)
  }

  test("Appending after switching to seq") {
    val a = new SimpleSourceStage()
    val b = new SimpleTransformStage()
    val c = new SimpleTransformStage()

    builder.edge(a, b)
    builder.setPipelineType(PipelineType.Sequential)
    builder.append(c)

    assert(builder.graph.isSequential)
  }

  test("Cannot append same object twice") {
    val a = new SimpleSourceStage()

    builder.append(a)

    assertThrows[IllegalArgumentException] {
      builder.append(a)
    }
  }

  test("Append an anonymous input pipeline item") {
    val pipeline = builder
      .appendSource { x: StreamExecutionEnvironment =>
        x.fromCollection(List(StringType("Test")))
      }
      .append { x: DataStream[StringType] =>
        x.map(x => x)
      }
      .build()

    assert(pipeline.graph.nodes.size == 2)

    pipeline.graph.nodes.head shouldBe an[Stage[Nothing, StringType]]
    pipeline.graph.nodes.last shouldBe an[Stage[StringType, StringType]]
  }

  test("Append an anonymous pipeline item") {
    val pipeline = builder
      .append(new SimpleSourceStage())
      .append { x: DataStream[StringType] =>
        x.map(x => x)
      }
      .build()

    assert(pipeline.graph.nodes.size == 2)

    pipeline.graph.nodes.head shouldBe an[SimpleSourceStage]
    pipeline.graph.nodes.last shouldBe an[Stage[StringType, StringType]]
  }

  test("Append an anonymous pipeline job") {
    val pipeline = builder
      .append(new SimpleSourceStage())
      .append { x: DataStream[StringType] =>
        x.addSink(new SinkFunction[StringType] {})
      }
      .build()

    assert(pipeline.graph.nodes.size == 2)

    pipeline.graph.nodes.head shouldBe an[SimpleSourceStage]
    pipeline.graph.nodes.last shouldBe an[OutputStage[StringType]]
  }

  test("Should add parents when using addParents") {
    val a = new SimpleSourceStage()
    val b = new SimpleTransformStage()
    val c = new SimpleTransformStage()

    val pipeline = builder
      .disablePipelineVerification()
      .addParents(c, a :+ b)
      .build()

    pipeline.graph.getParents(c)(0) shouldBe an[SimpleSourceStage]
    pipeline.graph.getParents(c)(1) shouldBe an[SimpleTransformStage]
  }

  test("Using add parents when objects already exists only adds edges") {
    val a = new SimpleSourceStage()
    val b = new SimpleTransformStage()
    val c = new SimpleTransformStage()

    val pipeline = builder
      .disablePipelineVerification()
      .addParents(c, a :+ b)
      .addParents(c, a)
      .build()

    pipeline.graph.getParents(c)(0) shouldBe an[SimpleSourceStage]
    pipeline.graph.getParents(c)(1) shouldBe an[SimpleTransformStage]
    assert(pipeline.graph.getParents(c).size == 2)
  }

  test("Set the serializer directly") {
    val pipeline = builder
      .append(new SimpleSourceStage())
      .setSerializer(Serializer.JSON4s)
      .build()

    assert(pipeline.bufferProperties.get(Buffer.SERIALIZER).get == "JSON4s")
  }

  test("Next level") {
    val a = new SimpleSourceStage(Some("testId"))
    val b = new SimpleTransformStage()

    val pipeline = builder
      .append(a)
      //      .addStage(a)
      //      .usingId("AnId")
      //      .setProperty("foo", "bar")
      //      .setProperty("hello", "world")
      .build()

    assert(a.getContext.stageId == "testId")
  }
}
