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

import com.sksamuel.avro4s.FromRecord
import org.apache.flink.streaming.api.scala.DataStream
import org.codefeedr.Properties
import org.codefeedr.keymanager.KeyManager
import org.codefeedr.pipeline.PipelineType.PipelineType
import org.codefeedr.pipeline.buffer.BufferType
import org.codefeedr.pipeline.buffer.BufferType.BufferType

import scala.collection.mutable
import scala.reflect.ClassTag

class PipelineBuilder() {
  /** Type of buffer used in the pipeline */
  protected var bufferType: BufferType = BufferType.None

  /** Type of the pipeline graph */
  protected var pipelineType: PipelineType = PipelineType.Sequential

  /** Properties of the buffer */
  protected[pipeline] var bufferProperties = new Properties()

  /** Stage properties */
  protected val stageProperties = new mutable.HashMap[String, Properties]()

  /** Key manager */
  protected var keyManager: KeyManager = _

  /** Graph of the pipeline */
  protected[pipeline] var graph = new DirectedAcyclicGraph()

  /** Last inserted pipeline obejct, used to convert sequential to dag. */
  private var lastObject: AnyRef = _

  /**
    * Get the type of the buffer
    * @return buffer type
    */
  def getBufferType: BufferType = {
    bufferType
  }

  /**
    * Set the type of the buffer
    *
    * @param bufferType New type
    * @return This builder
    */
  def setBufferType(bufferType: BufferType): PipelineBuilder = {
    this.bufferType = bufferType

    this
  }

  /**
    * Get the type of the pipeline
    * @return Type of pipeline
    */
  def getPipelineType: PipelineType= {
    pipelineType
  }

  /**
    * Set the type of the pipeline
    * @param pipelineType Type of the pipeline
    * @return This builder
    */
  def setPipelineType(pipelineType: PipelineType): PipelineBuilder = {
    if (pipelineType == PipelineType.Sequential && this.pipelineType == PipelineType.DAG) {
      if (!graph.isSequential) {
        throw new IllegalStateException("The current non-sequential pipeline can't be turned into a sequential pipeline")
      }

      lastObject = graph.lastInSequence.get
    }

    this.pipelineType = pipelineType

    this
  }

  /**
    * Set a buffer property
    *
    * A buffer property is generic for all buffers
    *
    * @param key Key
    * @param value Value
    * @return
    */
  def setBufferProperty(key: String, value: String): PipelineBuilder = {
    bufferProperties = bufferProperties.set(key, value)

    this
  }

  def setStageProperty(id: String, key: String, value: String): PipelineBuilder = {
    val properties = stageProperties.getOrElse(id, new Properties())

    stageProperties.put(id, properties.set(key, value))

    this
  }

  /**
    * Set a key manager.
    *
    * A key manager handles API key management for sources.
    *
    * @param km Key manager
    * @return This builder
    */
  def setKeyManager(km: KeyManager): PipelineBuilder = {
    keyManager = km

    this
  }

  /**
    * Append a node to the sequential pipeline.
    */
  def append[U <: PipelineItem, V <: PipelineItem](item: PipelineObject[U, V]): PipelineBuilder = {
    if (pipelineType != PipelineType.Sequential) {
      throw new IllegalStateException("Can't append node to non-sequential pipeline")
    }

    if (graph.hasNode(item)) {
      throw new IllegalArgumentException("Item already in sequence.")
    }

    graph = graph.addNode(item)

    if (lastObject != null) {
      graph = graph.addEdge(lastObject, item)
    }
    lastObject = item

    this
  }

  /**
    * Append a node created from a transform function
    *
    * @param trans Function
    * @return Builder
    */
  def append[U <: PipelineItem : ClassTag : Manifest : FromRecord, V <: PipelineItem : ClassTag : Manifest : FromRecord](trans : DataStream[U] => DataStream[V]): PipelineBuilder = {
    val pipelineItem = new PipelineObject[U, V](null) {
      override def transform(source: DataStream[U]): DataStream[V] = trans(source)
    }

    append(pipelineItem)
  }

  /**
    * Append a node created from an output function
    *
    * @param trans Function
    * @return Builder
    */
  def append[U <: PipelineItem : ClassTag : Manifest : FromRecord](trans : DataStream[U] => Any): PipelineBuilder = {
    val pipelineItem = new OutputStage[U](null) {
      override def main(source: DataStream[U]): Unit = trans(source)
    }

    append(pipelineItem)
  }

  private def makeEdge[U <: PipelineItem, V <: PipelineItem, X <: PipelineItem, Y <: PipelineItem](from: PipelineObject[U, V], to: PipelineObject[X, Y], checkEdge: Boolean = true): Unit = {
    if (pipelineType != PipelineType.DAG) {
      if (!graph.isEmpty) {
        throw new IllegalStateException("Can't append node to non-sequential pipeline")
      }

      pipelineType = PipelineType.DAG
    }

    if (!graph.hasNode(from)) {
      graph = graph.addNode(from)
    }

    if (!graph.hasNode(to)) {
      graph = graph.addNode(to)
    }

    if (checkEdge && graph.hasEdge(from, to)) {
      throw new IllegalArgumentException("Edge in graph already exists")
    }

    graph = graph.addEdge(from, to)
  }

  /**
    * Create an edge between two sources in a DAG pipeline. The 'to' must not already have a parent.
    *
    * If the graph is not configured yet (has no nodes), the graph is switched to a DAG automatically. If it was
    * already configured as sequential, it will throw an illegal state exception.
    */
  def edge[U <: PipelineItem, V <: PipelineItem, X <: PipelineItem, Y <: PipelineItem](from: PipelineObject[U, V], to: PipelineObject[X, Y]): PipelineBuilder = {
    makeEdge(from, to)

    this
  }

  /**
    * Add multiple parents in given ordered list.
    *
    * @param obj Node
    * @param parents Parents
    * @tparam U Node In
    * @tparam V Node Out
    * @return Builder
    */
  def addParents[U <: PipelineItem, V <: PipelineItem](obj: PipelineObject[U, V], parents: PipelineObjectList): PipelineBuilder = {
    for (item <- parents) {
      makeEdge(item, obj, checkEdge = false)
    }

    this
  }

  /**
    * Add a parent.
    *
    * @param obj Node
    * @param parent Parent
    * @return
    */
  def addParents[U <: PipelineItem, V <: PipelineItem, X <: PipelineItem, Y <: PipelineItem](obj: PipelineObject[U, V], parent: PipelineObject[X, Y]): PipelineBuilder =
    addParents(obj, parent.inList)

  /**
    * Build a pipeline from the builder configuration
    *
    * @throws EmptyPipelineException When no pipeline is defined
    * @return Pipeline
    */
  def build(): Pipeline = {
    if (graph.isEmpty) {
      throw EmptyPipelineException()
    }

    graph.nodes.foreach(_.asInstanceOf[PipelineObject[PipelineItem, PipelineItem]].verifyGraph(graph))

    Pipeline(bufferType, bufferProperties, graph, keyManager, stageProperties.toMap)
  }
}
