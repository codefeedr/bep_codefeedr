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
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.DataStream
import org.codefeedr.Properties
import org.codefeedr.pipeline.buffer.BufferFactory

import scala.reflect.{ClassTag, Manifest}
import scala.reflect.runtime.universe._

/**
  * This class represents a processing job within the pipeline.
  *
  * @tparam In  input type for this pipeline object.
  * @tparam Out output type for this pipeline object.
  */
abstract class PipelineObject[In <: PipelineItem : ClassTag : Manifest : FromRecord, Out <: PipelineItem : ClassTag : Manifest : FromRecord](val attributes: StageAttributes = StageAttributes()) {

  var pipeline: Pipeline = _

  def id: String = attributes.id.getOrElse(getClass.getName)

  /**
    * Stage properties
    *
    * @return Properties
    */
  def properties: Properties =
    pipeline.propertiesOf(this)

  /**
    * Setups the pipeline object with a pipeline.
    *
    * @param pipeline the pipeline it belongs to.
    */
  def setUp(pipeline: Pipeline): Unit = {
    this.pipeline = pipeline
  }

  /**
    * Transforms the pipeline object from its input type to its output type.
    * This requires using the Flink DataStream API.
    *
    * @param source the input source.
    * @return the transformed stream.
    */
  def transform(source: DataStream[In]): DataStream[Out]

  /**
    * Verify that the object is valid.
    *
    * Checks types of the input sources and whether the graph is configured correctly for the types.
    */
  protected[pipeline] def verifyGraph(graph: DirectedAcyclicGraph): Unit = {}

  /**
    * Get all parents for this object
    *
    * @return set of parents. Can be empty
    */
  def getParents: Vector[PipelineObject[PipelineItem, PipelineItem]] =
    pipeline.graph.getParents(this).asInstanceOf[Vector[PipelineObject[PipelineItem, PipelineItem]]]

  /**
    * Check if this pipeline object is sourced from a Buffer.
    *
    * @return if this object has a (buffer) source.
    */
  def hasMainSource: Boolean =
    typeOf[In] != typeOf[NoType] && pipeline.graph.getFirstParent(this).isDefined

  /**
    * Check if this pipeline object is sinked to a Buffer.
    *
    * @return if this object has a (buffer) sink.
    */
  def hasSink: Boolean = typeOf[Out] != typeOf[NoType]

  /**
    * Returns the buffer source of this pipeline object.
    *
    * @return the DataStream resulting from the buffer.
    */
  def getMainSource: DataStream[In] = {
    assert(pipeline != null)

    if (!hasMainSource) {
      throw NoSourceException("PipelineObject defined NoType as In type. Buffer can't be created.")
    }

    val parentNode = getParents(0)

    val factory = new BufferFactory(pipeline, this, parentNode)
    val buffer = factory.create[In]()

    buffer.getSource
  }

  /**
    * Returns the buffer sink of this pipeline object.
    *
    * @return the SinkFunction resulting from the buffer.
    */
  def getSink: SinkFunction[Out] = {
    assert(pipeline != null)

    if (!hasSink) {
      throw NoSinkException("PipelineObject defined NoType as Out type. Buffer can't be created.")
    }

    val factory = new BufferFactory(pipeline, this,this)
    val buffer = factory.create[Out]()

    buffer.getSink
  }

  /**
    * Get the sink subject used by the buffer.
    *
    * This is also used for child objects to read from the buffer again.
    *
    * @return Sink subject
    */
  def getSinkSubject: String = this.getClass.getName

  def getSource[T <: AnyRef : Manifest : FromRecord](parentNode: PipelineObject[PipelineItem, PipelineItem]): DataStream[T] = {
    assert(parentNode != null)

    val factory = new BufferFactory(pipeline, this, parentNode)
    val buffer = factory.create[T]()

    buffer.getSource
  }

  /**
    * Create a list of object by appending another object
    *
    * @param obj Other object
    * @return List with this and other
    */
  def :+[U <: PipelineItem, V <: PipelineItem](obj: PipelineObject[U, V]): PipelineObjectList =
    inList.add(obj)

  /**
    * Create a list witht his object
    *
    * @return List
    */
  def inList: PipelineObjectList =
    new PipelineObjectList().add(this)
}
