package org.codefeedr.pipeline

import org.codefeedr.{DirectedAcyclicGraph, Properties}
import org.codefeedr.keymanager.KeyManager
import org.codefeedr.pipeline.PipelineType.PipelineType
import org.codefeedr.pipeline.buffer.BufferType
import org.codefeedr.pipeline.buffer.BufferType.BufferType

class PipelineBuilder() {
  /** Type of buffer used in the pipeline */
  protected var bufferType: BufferType = BufferType.None

  /** Type of the pipeline graph */
  protected var pipelineType: PipelineType = PipelineType.Sequential

  /** Properties of the buffer */
  val bufferProperties = new Properties()

  /** Pipeline properties */
  val properties = new Properties()

  /** Key manager */
  protected var keyManager: KeyManager = _

  /** Graph of the pipeline */
  protected var graph = new DirectedAcyclicGraph()

  /** Last inserted pipeline obejct, used to convert sequential to dag. */
  private var lastObject: AnyRef = _


  def getBufferType: BufferType = {
    bufferType
  }

  def setBufferType(bufferType: BufferType): PipelineBuilder = {
    this.bufferType = bufferType

    this
  }

  def getPipelineType: PipelineType= {
    pipelineType
  }

  def setPipelineType(pipelineType: PipelineType): PipelineBuilder = {
    this.pipelineType = pipelineType

    this
  }

  /**
    * Append a node to the sequential pipeline.
    */
  def append[U <: PipelinedItem, V <: PipelinedItem](item: PipelineObject[U, V]): PipelineBuilder = {
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
    * Create an edge between two sources in a DAG pipeline.
    *
    * If the graph is not configured yet (has no nodes), the graph is switched to a DAG automatically. If it was
    * already configured as sequential, it will throw an illegal state exception.
    */
  def edge[U <: PipelinedItem, V <: PipelinedItem, X <: PipelinedItem, Y <: PipelinedItem](from: PipelineObject[U, V], to: PipelineObject[X, Y]): PipelineBuilder = {
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

    if (graph.hasEdge(from, to)) {
      throw new IllegalStateException("Edge in graph already exists")
    }

    graph = graph.addEdge(from, to)

    this
  }

  def setProperty(key: String, value: String): PipelineBuilder = {
    properties.set(key, value)

    this
  }

  def setBufferProperty(key: String, value: String): PipelineBuilder = {
    bufferProperties.set(key, value)

    this
  }

  def setKeyManager(km: KeyManager): PipelineBuilder = {
    keyManager = km

    this
  }

  def build(): Pipeline = {
    if (this.graph.isEmpty) {
      throw EmptyPipelineException()
    }

    Pipeline(bufferType, bufferProperties.toImmutable, graph.withoutOrphans , properties.toImmutable, keyManager)
  }
}
