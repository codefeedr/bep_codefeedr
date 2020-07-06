package org.codefeedr.plugins.clearlydefined.operators

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.codefeedr.plugins.clearlydefined.protocol.Protocol
import org.codefeedr.plugins.clearlydefined.protocol.Protocol.ClearlyDefinedRelease
import org.codefeedr.stages.utilities.{HttpRequester, RequestException}
import org.codefeedr.pipeline.{PluginReleasesSource, PluginSourceConfig}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization.read
import scalaj.http.Http

/**
  * The configuration class for the ClearlyDefinedReleasesSource class
  * @param pollingInterval Amount of milliseconds to wait for next poll. Put to 30 seconds due to typically slow feed
  * @param maxNumberOfRuns if positive, runs definitely up till x. If negative, runs indefinitely.
  */
case class ClearlyDefinedSourceConfig(pollingInterval: Int = 30000,
                                      maxNumberOfRuns: Int = -1,
                                      timeout: Int = 32)
    extends PluginSourceConfig

/**
  * Important to note in retrieving data from the stream of projects in ClearlyDefined is the following:
  *  - The stream URL is https://api.clearlydefined.io/definitions?matchCasing=false&sort=releaseDate&sortDesc=true
  *  - 100 most recently changed packages can be found there, which is way too much to process with each poll
  *  - Therefore only the first {packageAmount} number of packages are processed
  * @param config the ClearlyDefined source configuration, has pollingInterval and maxNumberOfRuns fields
  */
class ClearlyDefinedReleasesSource(
    config: ClearlyDefinedSourceConfig = ClearlyDefinedSourceConfig()
) extends PluginReleasesSource[ClearlyDefinedRelease](config) {

  /** url for the stream of new CD projects */
  val url =
    "https://api.clearlydefined.io/definitions?matchCasing=false&sort=releaseDate&sortDesc=true"

  /** The first x number of packages to process with each poll */
  val packageAmount = 10

  /**
    * Main fetcher of new items in the ClearlyDefined package source
    * @param ctx context
    */
  override def run(
      ctx: SourceFunction.SourceContext[ClearlyDefinedRelease]
  ): Unit = {
    val lock = ctx.getCheckpointLock

    /** While is running or #runs left. */
    while (isRunning && runsLeft != 0) {
      lock.synchronized { // Synchronize to the checkpoint lock.
        try {
          // Polls the RSS feed
          val rssAsString: String = getRSSAsString.get
          // Parses the received rss items
          val items: Seq[ClearlyDefinedRelease] = parseRSSString(rssAsString)
          // Collect right items and update last item
          val validSortedItems: Seq[ClearlyDefinedRelease] =
            sortAndDropDuplicates(items)
          // Decrease runs left
          super.decreaseRunsLeft()
          // Add a timestamp to the item
          validSortedItems.foreach(
            x =>
              ctx.collectWithTimestamp(
                x,
                Protocol.getDate(x._meta.updated).getTime
            ))
          // Call run in parent
          super.runPlugin(ctx, validSortedItems)
        } catch {
          case _: Throwable =>
        }
      }
    }
  }

  /**
    * Drops items that already have been collected and sorts them based on times
    * TODO: x._meta.updated is not chronological ~5% of the time, which means 1 in 20 packages are SKIPPED
    *
    * @param items Potential items to be collected
    * @return Valid sorted items
    */
  def sortAndDropDuplicates(
      items: Seq[ClearlyDefinedRelease]
  ): Seq[ClearlyDefinedRelease] = {
    items
      .filter((x: ClearlyDefinedRelease) => {
        if (lastItem.isDefined)
          Protocol
            .getDate(lastItem.get._meta.updated)
            .before(Protocol.getDate(x._meta.updated))
        else
          true
      })
      .sortWith((x: ClearlyDefinedRelease, y: ClearlyDefinedRelease) =>
        Protocol
          .getDate(x._meta.updated)
          .before(Protocol.getDate(y._meta.updated)))
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
    * Parses a string that contains JSON with RSS items into a list of ClearlyDefinedRelease's
    * @param rssString JSON string which contains RSS items
    * @return
    */
  def parseRSSString(rssString: String): Seq[ClearlyDefinedRelease] = {
    try {
      // Parse the big release string as a Json object
      val json: JValue = parse(rssString)

      // Retrieve the first {packageAmount} packages
      val packages: List[JValue] =
        (json \ "data").children.take(this.packageAmount)

      // Render back to string to prepare for the 'read' call (requires string input)
      val packagesString: List[String] = packages.map(x => compact(render(x)))

      // Convert from string to ClearlyDefinedRelease
      implicit val formats = DefaultFormats
      for (packageString <- packagesString) yield {
        read[ClearlyDefinedRelease](packageString)
      }
    } catch {
      // If the string cannot be parsed return an empty list
      case _: Throwable =>
        printf(
          "Failed parsing the RSSString in the ClearlyDefinedReleasesSource.scala file"
        )
        Nil
    }
  }
}
