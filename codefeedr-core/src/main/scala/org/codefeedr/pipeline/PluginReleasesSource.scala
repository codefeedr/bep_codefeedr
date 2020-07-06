package org.codefeedr.pipeline

import org.apache.flink.api.common.accumulators.LongCounter
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.{
  FunctionInitializationContext,
  FunctionSnapshotContext
}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.source.{
  RichSourceFunction,
  SourceFunction
}

import scala.collection.JavaConverters._
import scala.reflect.{ClassTag, _}

abstract class PluginSourceConfig {
  def pollingInterval: Int
  def maxNumberOfRuns: Int
}

abstract class PluginReleasesSource[T: ClassTag](config: PluginSourceConfig)
    extends RichSourceFunction[T]
    with CheckpointedFunction {

  /**
    * Indication of operation status, i.e. running or not
    */
  protected var isRunning: Boolean = false

  /**
    * @return whether this source is running or not
    */
  def getIsRunning: Boolean = isRunning

  /**
    * Counter to indicate the number of polls left to poll the update stream
    */
  protected var runsLeft: Int = 0

  /**
    * Accumulator for the amount of processed releases.
    */
  protected val releasesProcessed = new LongCounter()

  /**
    * Checkpointed last release processed
    */
  protected var lastItem: Option[T] = None

  /**
    * Keeps track of a checkpointed state of releases
    */
  @transient
  protected var checkpointedState: ListState[T] = _
  def getCheckpointedstate: ListState[T] = checkpointedState

  /** Opens this source. */
  override def open(parameters: Configuration): Unit = {
    isRunning = true
    runsLeft = config.maxNumberOfRuns
  }

  /** Closes this source. */
  override def cancel(): Unit = {
    isRunning = false
  }

  /**
    * Wait a certain amount of times the polling interval
    *
    * @param times Times the polling interval should be waited
    */
  def waitPollingInterval(times: Int = 1): Unit = {
    Thread.sleep(times * config.pollingInterval)
  }

  /**
    * Reduces runsLeft by 1
    */
  def decreaseRunsLeft(): Unit = {
    if (runsLeft > 0) {
      runsLeft -= 1
    }
  }

  /** Make a snapshot of the current state. */
  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    if (lastItem.isDefined) {
      checkpointedState.clear()
      checkpointedState.add(lastItem.get)
    }
  }

  /**
    * Cycles through the polling of the plugin's run method; Does generic functionality for all plugins
    * @param ctx context
    */
  def runPlugin(
      ctx: SourceFunction.SourceContext[T],
      validSortedItems: Seq[T]
  ): Unit = {
    releasesProcessed.add(validSortedItems.size)
    if (validSortedItems.nonEmpty) {
      lastItem = Some(validSortedItems.last)
    }

    // Wait until the next poll
    waitPollingInterval()
  }

  /** Initializes state by reading from a checkpoint or creating an empty one. */
  override def initializeState(context: FunctionInitializationContext): Unit = {
    val descriptor = new ListStateDescriptor[T](
      "last_element",
      classTag[T].runtimeClass.asInstanceOf[Class[T]]
    )

    checkpointedState = context.getOperatorStateStore.getListState(descriptor)

    if (context.isRestored) {
      checkpointedState.get().asScala.foreach { x =>
        lastItem = Some(x)
      }
    }
  }
}
