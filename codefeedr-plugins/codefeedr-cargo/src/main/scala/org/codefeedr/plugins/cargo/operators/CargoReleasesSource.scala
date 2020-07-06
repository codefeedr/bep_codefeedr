package org.codefeedr.plugins.cargo.operators

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.codefeedr.pipeline.{PluginReleasesSource, PluginSourceConfig}
import org.codefeedr.stages.utilities.{HttpRequester, RequestException}
import org.codefeedr.plugins.cargo.protocol.Protocol.{
  CrateFromPoll,
  CrateRelease
}
import scalaj.http.Http
import spray.json._

case class CargoSourceConfig(pollingInterval: Int = 10000,
                             maxNumberOfRuns: Int = -1,
                             timeout: Int = 32)
    extends PluginSourceConfig

/**
  * Important to note in retrieving data from the stream of crates in Cargo is the following:
  *  - The stream URL is https://crates.io/api/v1/summary
  *  - A list of 10 of the most recent crates can be found there under the name "new_crates"
  *  - This information per crate is minimal, so the id/name is taken and used in a separate URL
  *  - This URL is https://crates.io/api/v1/crates/{name}
  * @param config the cargo source configuration, has pollingInterval and maxNumberOfRuns fields
  */
class CargoReleasesSource(config: CargoSourceConfig = CargoSourceConfig())
    extends PluginReleasesSource[CrateRelease](config) {

  /** url for the stream of updated and new crates */
  val url = "https://crates.io/api/v1/summary"

  /**
    * Main fetcher of new items in the Crates.io package source
    * @param ctx context
    */
  override def run(ctx: SourceFunction.SourceContext[CrateRelease]): Unit = {
    val lock = ctx.getCheckpointLock

    /** While is running or #runs left. */
    while (isRunning && runsLeft != 0) {
      lock.synchronized { // Synchronize to the checkpoint lock.
        try {
          // Polls the RSS feed
          val rssAsString: String = getRSSAsString.get
          // Parse into polled micro-crates
          val polledItems: Seq[CrateFromPoll] = getPolledCrates(rssAsString)
          // Collect only new crates and sort them based on updated date
          val validSortedItems: Seq[CrateFromPoll] =
            sortAndDropDuplicates(polledItems)
          // Parses the polled crates into full crates
          val items: Seq[CrateRelease] = parseNewCrates(validSortedItems)
          // Decrease runs left
          super.decreaseRunsLeft()
          // Add a timestamp to the item
          items.foreach(x =>
            ctx.collectWithTimestamp(x, x.crate.updated_at.getTime))
          // Call the parent run
          super.runPlugin(ctx, items)
        } catch {
          case _: Throwable =>
        }
      }
    }
  }

  /**
    * Drops items that already have been collected and sorts them based on times
    *
    * @param items Potential items to be collected
    * @return Valid sorted items
    */
  def sortAndDropDuplicates(items: Seq[CrateFromPoll]): Seq[CrateFromPoll] = {
    items
      .filter((x: CrateFromPoll) => {
        if (lastItem.isDefined)
          lastItem.get.crate.updated_at.before(x.updated_at)
        else
          true
      })
      .sortWith((x: CrateFromPoll, y: CrateFromPoll) =>
        x.updated_at.before(y.updated_at))
  }

  /**
    * Requests the RSS feed and returns its body as a string.
    * Will keep trying with increasing intervals if it doesn't succeed
    *
    * @return Body of requested RSS feed
    */
  @throws[RequestException]
  def getRSSAsString: Option[String] = {
    try {
      Some(new HttpRequester().retrieveResponse(Http(url)).body)
    } catch {
      case _: Throwable => None
    }
  }

  /**
    * Gets the body response of a specific crate as String
    * @param crateName Name of the crate
    * @return Http Response body
    */
  def getRSSFromCrate(crateName: String): Option[String] = {
    try {
      Some(
        new HttpRequester()
          .retrieveResponse(
            Http("https://crates.io/api/v1/crates/".concat(crateName))
          )
          .body
      )
    } catch {
      case _: Throwable => None
    }
  }

  /**
    * Processes the html body from the Cargo feed into a list of polled Crates
    * @param rssString html body of the webrequest to the releases feed
    * @return A list of polled crates, consisting of 10 new crates and 10 updated crates
    */
  def getPolledCrates(rssString: String): Seq[CrateFromPoll] = {
    try {
      // Parse the big release string as a Json object
      val json: JsObject = rssString.parseJson.asJsObject

      // Retrieve 2x10 JsObjects of Crates
      val newCrates: Vector[JsObject] =
        JsonParser.getNewOrUpdatedCratesFromSummary(json, "new_crates").get
      val updatedCrates: Vector[JsObject] =
        JsonParser.getNewOrUpdatedCratesFromSummary(json, "just_updated").get

      // Translate 2x10 JSObjects into CrateFromPolls
      for (crate <- newCrates ++ updatedCrates) yield {
        val crateId: String =
          JsonParser.getStringFieldFromCrate(crate, "id").get
        val crateUpdated =
          JsonParser.getDateFieldFromCrate(crate, "updated_at").get
        CrateFromPoll(crateId, crateUpdated)
      }
    } catch {
      // If the string cannot be parsed return an empty list
      case _: Throwable =>
        printf(
          "Failed parsing the RSSString in the CargoReleasesSource.scala file"
        )
        Nil
    }
  }

  /**
    * Parses a string that contains polled crates into a list of CrateReleases
    *
    * @param newCrates sequence of polled crates to turn into extended releases
    * @return Sequence of RSS items in type CrateRelease
    */
  def parseNewCrates(newCrates: Seq[CrateFromPoll]): Seq[CrateRelease] = {
    try {
      for (crate <- newCrates) yield {
        // Get the html body of the crate
        val crateRSS: String = getRSSFromCrate(crate.id).get
        // Turn the html body into a json object
        val crateJson: JsObject = crateRSS.parseJson.asJsObject
        // Turn the json object into a CrateRelease object
        JsonParser.parseCrateJsonToCrateRelease(crateJson).get
      }
    } catch {
      // If the string cannot be parsed return an empty list
      case _: Throwable =>
        printf(
          "Failed parsing the RSSString in the CargoReleasesSource.scala file"
        )
        Nil
    }
  }
}
