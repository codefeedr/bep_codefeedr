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
package org.codefeedr.pipeline

import org.apache.flink.api.common.restartstrategy.RestartStrategies.RestartStrategyConfiguration
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic
import org.codefeedr.Properties
import org.codefeedr.buffer.BufferType.BufferType
import org.codefeedr.buffer.FakeStage
import org.codefeedr.keymanager.KeyManager
import org.codefeedr.pipeline.RuntimeType.RuntimeType

/** Properties of a pipeline are stored in this case class.
  *
  * @param bufferType The type of [[org.codefeedr.buffer.Buffer]] (e.g. Kafka).
  * @param bufferProperties The properties of the Buffer.
  * @param keyManager The key manager which provide API call management at stage-level.
  * @param streamTimeCharacteristic The TimeCharacteristic of the whole pipeline. Event, Ingestion or Processing.
  * @param restartStrategy The RestartStrategy of the whole pipeline.
  * @param checkpointing Captures if checkpointing is enabled and if so, what the interval is.
  */
case class PipelineProperties(bufferType: BufferType,
                              bufferProperties: Properties,
                              keyManager: KeyManager,
                              streamTimeCharacteristic: TimeCharacteristic,
                              restartStrategy: RestartStrategyConfiguration,
                              checkpointing: Option[Long],
                              checkpointingMode: CheckpointingMode,
                              stateBackend: StateBackend)

/** The Pipeline holds all the data and logic to execute a CodeFeedr job.
  * It stores all stages (Flink jobs) and connects them by setting up buffers (like Kafka).
  *
  * @param name The name of the pipeline.
  * @param graph The graph of stages (nodes) and edges (buffers).
  * @param objectProperties the properties of each stage.
  */
case class Pipeline(var name: String,
                    pipelineProperties: PipelineProperties,
                    graph: DirectedAcyclicGraph,
                    objectProperties: Map[String, Properties]) {

  /** Prepare the pipeline by adding this instance to every stage.*/
  def prepare() = {
    val nodes = getNodes

    // Run all setups.
    for (obj <- nodes) {
      obj.setUp(this)
    }

  }

  /** Run this immediately. */
  prepare()

  /** The mutable StreamExecutionEnvironment. */
  var _environment: StreamExecutionEnvironment = null

  /** Immutable StreamExecutionEnvironment.
    *
    * By default the [[TimeCharacteristic]] is set to EvenTime.
    */
  val environment: StreamExecutionEnvironment = {
    if (_environment == null) {
      _environment = StreamExecutionEnvironment.getExecutionEnvironment
      _environment.setStreamTimeCharacteristic(
        pipelineProperties.streamTimeCharacteristic)
      _environment.setRestartStrategy(pipelineProperties.restartStrategy)
      _environment.setStateBackend(pipelineProperties.stateBackend)

      if (pipelineProperties.checkpointing.isDefined) {
        _environment.enableCheckpointing(pipelineProperties.checkpointing.get,
                                         pipelineProperties.checkpointingMode)
      }
    }

    _environment
  }

  /** Auxiliary method to retrieve buffer properties. */
  def bufferProperties = pipelineProperties.bufferProperties

  /** Auxiliary method to retrieve buffer type. */
  def bufferType = pipelineProperties.bufferType

  /** Auxiliary method to retrieve key manager. */
  def keyManager = pipelineProperties.keyManager

  /** Get the properties of a stage.
    *
    * @param stage Stage The stage to retrieve properties from.
    * @return Properties The obtained properties.
    */
  def propertiesOf[U <: Serializable with AnyRef,
                   V <: Serializable with AnyRef](
      stage: Stage[U, V]): Properties = {
    if (stage == null) {
      throw new IllegalArgumentException("Stage can't be null")
    }

    objectProperties.getOrElse(stage.id, new Properties())
  }

  /** Start the pipeline with a list of command line arguments.
    *
    * Three types of run-modes are available:
    * - Mock: Flink Jobs are linked without buffer, this requires the pipeline to be sequential.
    * - Local: All Flink jobs are run in one [[StreamExecutionEnvironment]] connected with buffers.
    * - Cluster: Flink jobs are run separately and connected with buffers.
    *
    * @param args Array of command line arguments.
    */
  def start(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)

    // By default it is local.
    var runtime = RuntimeType.Local

    // If a stage is specified, set to cluster-mode.
    val stage = params.get("stage")
    if (stage != null) {
      runtime = RuntimeType.Cluster
    }

    // Override runtime with runtime parameter.
    runtime = params.get("runtime") match {
      case "mock"    => RuntimeType.Mock
      case "local"   => RuntimeType.Local
      case "cluster" => RuntimeType.Cluster
      case _         => runtime
    }

    // Set name if specified.
    name = params.get("name", name)

    if (params.has("list") || runtime == RuntimeType.Cluster) {
      validateUniqueness() // make sure stage id's are unique.
    }

    if (params.has("list")) {
      showList(params.has("asException"))
    } else {
      start(runtime, stage, params.get("groupId"))
    }
  }

  /** Get all the nodes/stages in the pipeline. */
  private def getNodes
    : Vector[Stage[Serializable with AnyRef, Serializable with AnyRef]] =
    graph.nodes.asInstanceOf[Vector[
      Stage[Serializable with AnyRef, Serializable with AnyRef]]]

  /** Validates the uniqueness of the stage IDs, needed for clustered running. */
  def validateUniqueness(): Unit = {
    val list = getNodes.map(_.getContext.stageId)
    val uniqList = list.distinct
    val overlap = list.diff(uniqList)

    // If overlap, throw exception.
    if (overlap.nonEmpty) {
      throw StageIdsNotUniqueException(overlap.head)
    }
  }

  /** Shows a list of stages inside the pipeline. Option to throw an exception to get the data through Flink.
    *
    * @param asException Throws the list as an exception.
    */
  def showList(asException: Boolean): Unit = {
    if (asException) {
      val contents = getNodes
        .filter(!_.isInstanceOf[FakeStage[_]])
        .map { item =>
          '"' + item.getContext.stageId + '"'
        }
        .mkString(",")
      val json = s"[$contents]"

      throw PipelineListException(json)
    } else {
      getNodes
        .filter(!_.isInstanceOf[FakeStage[_]])
        .foreach(item => println(item.getContext.stageId))
    }
  }

  /** Start the pipeline with a run configuration.
    *
    * @param runtime Runtime mode (mock, local, clustered).
    * @param stage   Stage to start in case of a clustered run.
    * @param groupId Group id of the stage (by default set to stage id).
    */
  def start(runtime: RuntimeType,
            stage: String = null,
            groupId: String = null): Unit = {
    runtime match {
      case RuntimeType.Mock    => startMock()
      case RuntimeType.Local   => startLocal()
      case RuntimeType.Cluster => startClustered(stage, groupId)
    }
  }

  /** Run the pipeline as mock. Only works for sequential pipelines.
    *
    * In a mock run, all stages are put together without buffers and run as a single Flink job.
    */
  def startMock(): Unit = {
    if (!graph.isSequential) {
      throw new IllegalStateException(
        "Mock mode can't run non-sequential pipelines.")
    }

    val nodes = getNodes

    // Connect each object by getting a starting buffer, if any, and sending it to the next.
    var buffer: DataStream[Serializable with AnyRef] = null
    for (obj <- nodes) {
      if (!obj.isInstanceOf[FakeStage[_]])
        buffer = obj.transform(buffer)
    }

    // Execute in one environment.
    environment.execute(s"$name: mock")
  }

  /** Start a locally run pipeline.
    * This mode starts every stage in the same Flink environment but with buffers.
    */
  def startLocal(): Unit = {
    val nodes = getNodes

    // For each PO, make buffers and run.
    for (obj <- nodes) {
      runStage(obj)
    }

    // Execute in one environment.
    environment.execute(s"$name: local")
  }

  /** Run the pipeline in a clustered manner: run a single stage only.
    *
    * @param stage Stage to run.
    * @param groupId GroupId to set.
    */
  def startClustered(stage: String, groupId: String = null): Unit = {
    // Find correct stage.
    val optObj = graph.nodes.find { node =>
      node
        .asInstanceOf[Stage[Serializable with AnyRef, Serializable with AnyRef]]
        .getContext
        .stageId == stage
    }

    // If cannot be find, throw an exception.
    if (optObj.isEmpty) {
      throw StageNotFoundException(s"Stage with name: $stage not found.")
    }

    // Cast to a stage.
    val obj = optObj.get
      .asInstanceOf[Stage[Serializable with AnyRef, Serializable with AnyRef]]

    // Run object.
    runStage(obj, groupId)

    // Run stage in one environment.
    environment.execute(s"$name: ${stage}")
  }

  /** Runs a stage.
    * Creates a source and sink for the object and then runs the transform function.
    *
    * @param stage The stage to run.
    * @param groupId The group id to set for the stage.
    */
  private def runStage(
      stage: Stage[Serializable with AnyRef, Serializable with AnyRef],
      groupId: String = null): Unit = {

    /** We won't run a fake stage. */
    if (stage.isInstanceOf[FakeStage[_]]) {
      return
    }

    // Find the source lazily.
    lazy val source =
      if (stage.hasMainSource) stage.getMainSource(groupId) else null

    // Find the sink lazily.
    lazy val sink = if (stage.hasSink) stage.getSink(groupId) else null

    // Add source.
    val transformed = stage.transform(source)

    // Add sink.
    if (sink != null) {
      transformed.addSink(sink).name(s"Producing to topic ${stage.stageId.get}")
    }
  }

}
