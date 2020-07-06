package org.codefeedr.plugins.npm.operators

import java.util.{Calendar, Date}

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.codefeedr.pipeline.{PluginReleasesSource, PluginSourceConfig}
import org.codefeedr.plugins.npm.protocol.Protocol.NpmRelease
import org.codefeedr.stages.utilities.HttpRequester
import scalaj.http.Http

/**
  * Configuration parameters for connecting to the NPM Packages data source
  *
  * @param pollingInterval the milliseconds between consecutive polls
  * @param maxNumberOfRuns the maximum number of polls executed
  *
  * @author Roald van der Heijden
  * Date: 2019 - 12 - 03
  */
case class NpmSourceConfig(pollingInterval: Int = 10000,
                           maxNumberOfRuns: Int = -1,
                           timeout: Int = 32)
    extends PluginSourceConfig

/**
  * Class to represent a source in CodeFeedr to query NPM package releases
  *
  * This files gets information on NPM packages from "https://npm-update-stream.libraries.io/"
  * Then queries "http://registry.npmjs.com/%packagenameWithPossibleScope%
  * to acquire the information for each specific package
  *
  * @param config the configuration paramaters object for this specific data source
  *
  * @author Roald van der Heijden
  * Date: 2019 - 12 - 03
  */
class NpmReleasesSource(config: NpmSourceConfig = NpmSourceConfig())
    extends PluginReleasesSource[NpmRelease](config) {

  /**
    * URL to get update stream from
    */
  val url_updatestream = "https://npm-update-stream.libraries.io/"

  //var timeout = 2 // timeout set specifically to speed up testing

  /**
    * The latest poll
    */
  private var lastPoll: Option[Seq[NpmRelease]] = None

  /**
    * Runs the source.
    * @param ctx the source the context.
    */
  override def run(ctx: SourceFunction.SourceContext[NpmRelease]): Unit = {
    val lock = ctx.getCheckpointLock

    /* While is running or #runs left. */
    while (isRunning && runsLeft != 0) {
      lock.synchronized { // Synchronize to the checkpoint lock.
        try {
          // Polls the update stream
          val resultString = retrieveUpdateStringFrom(url_updatestream)
          val now = Calendar.getInstance().getTime
          // convert string with updated packages into list of updated packages
          val items: Seq[NpmRelease] =
            createListOfUpdatedNpmIdsFrom(resultString.get, now)
          // Sort and drop the duplicate releases
          val newItems: Seq[NpmRelease] = sortAndDropDuplicates(items, now)
          // Decrease runs left
          super.decreaseRunsLeft()
          // Timestamp each item
          newItems.foreach(x =>
            ctx.collectWithTimestamp(x, x.retrieveDate.getTime))
          // Update lastPoll
          if (items.nonEmpty) {
            lastPoll = Some(items)
          }
          // call parent run
          super.runPlugin(ctx, newItems)
        } catch {
          case _: Throwable =>
        }
      }
    }
  }

  /**
    * Drops items that already have been collected and sorts them based on names
    *
    * @param items Potential items to be collected
    * @return Valid sorted items
    */
  def sortAndDropDuplicates(items: Seq[NpmRelease],
                            now: Date): Seq[NpmRelease] = {
    // Collect right items and update last item
    if (lastPoll.isDefined) {
      items
        .map(x => x.name)
        .diff(lastPoll.get.map(x => x.name))
        .map(x => NpmRelease(x, now))
    } else {
      items
    }
  }

  /**
    * Requests the update stream and returns its body as a string.
    * Will keep trying with increasing intervals if it doesn't succeed
    *
    * @return Body of requested update stream
    */
  def retrieveUpdateStringFrom(urlEndpoint: String): Option[String] = {
    val response = try {
      val request = Http(urlEndpoint)
      return Some(
        new HttpRequester(config.timeout).retrieveResponse(request).body)
    } catch {
      case _: Throwable => None
    }
    response
  }

  /**
    * Turns a given string (following request response from Librarios.io's update stream) into npm ids
    * @param input the string containing the names of the releases
    * @return a sequence of NPMRelease case classes
    */
  def createListOfUpdatedNpmIdsFrom(input: String,
                                    time: Date): List[NpmRelease] = {
    val packageList = input
      .replace("\"", "")
      .replace("[", "")
      .replace("]", "")
      .split(",")
      .toList
    packageList match {
      case List("") => Nil
      case _        => packageList.map(arg => NpmRelease(arg, time))
    }
  }
}
