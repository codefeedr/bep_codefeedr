package org.codefeedr.plugins.maven.operators

import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.codefeedr.stages.utilities.{HttpRequester, RequestException}
import org.codefeedr.pipeline.{PluginReleasesSource, PluginSourceConfig}
import org.codefeedr.plugins.maven.protocol.Protocol.{Guid, MavenRelease}
import scalaj.http.Http

import scala.xml.XML

case class MavenSourceConfig(pollingInterval: Int = 60000, // 1 min interval
                             maxNumberOfRuns: Int = -1,
                             timeout: Int = 32)
    extends PluginSourceConfig

class MavenReleasesSource(config: MavenSourceConfig = MavenSourceConfig())
    extends PluginReleasesSource[MavenRelease](config) {
  // Date formats + URL
  val pubDateFormat = "EEE, dd MMM yyyy HH:mm:ss ZZ"
  val url = "https://mvnrepository.com/feeds/rss2.0.xml"

  /** Runs the source.
    *
    * @param ctx the source the context.
    */
  override def run(ctx: SourceFunction.SourceContext[MavenRelease]): Unit = {
    val lock = ctx.getCheckpointLock

    /** While is running or #runs left. */
    while (isRunning && runsLeft != 0) {
      lock.synchronized { // Synchronize to the checkpoint lock.
        try {
          // Polls the RSS feed
          val rssAsString = getRSSAsString
          // Parses the received rss items
          val items: Seq[MavenRelease] = parseRSSString(rssAsString)
          // Collect right items and update last item
          val validSortedItems = sortAndDropDuplicates(items)
          // Decrease runs left
          super.decreaseRunsLeft()
          // Timestamp the items
          validSortedItems.foreach(x =>
            ctx.collectWithTimestamp(x, x.pubDate.getTime))
          // Call parent run
          super.runPlugin(ctx, validSortedItems)
        } catch {
          case _: Throwable =>
        }
      }
    }
  }

  /**
    * Requests the RSS feed and returns its body as a string.
    * Will keep trying with increasing intervals if it doesn't succeed
    *
    * @return Body of requested RSS feed
    */
  @throws[RequestException]
  def getRSSAsString: String = {
    new HttpRequester().retrieveResponse(Http(url)).body
  }

  /**
    * Parses a string that contains xml with RSS items
    *
    * @param rssString XML string with RSS items
    * @return Sequence of RSS items
    */
  def parseRSSString(rssString: String): Seq[MavenRelease] = {
    try {
      val xml = XML.loadString(rssString)
      val nodes = xml \\ "item"
      for (t <- nodes) yield xmlToMavenRelease(t)
    } catch {
      // If the string cannot be parsed return an empty list
      case _: Throwable => Nil
    }
  }

  /**
    * Parses a xml node to a RSS item
    *
    * @param node XML node
    * @return RSS item
    */
  def xmlToMavenRelease(node: scala.xml.Node): MavenRelease = {
    val title = (node \ "title").text
    val link = (node \ "link").text
    val description = (node \ "description").text
    val formatterPub = new SimpleDateFormat(pubDateFormat)
    val pubDate = formatterPub.parse((node \ "pubDate").text)

    val tag = (node \ "guid").text
    MavenRelease(title, link, description, pubDate, Guid(tag))
  }

  /**
    * Drops items that already have been collected and sorts them based on times
    *
    * @param items Potential items to be collected
    * @return Valid sorted items
    */
  def sortAndDropDuplicates(items: Seq[MavenRelease]): Seq[MavenRelease] = {
    items
      .filter((x: MavenRelease) => {
        if (lastItem.isDefined) {
          lastItem.get.pubDate.before(x.pubDate) && lastItem.get.link != x.link
        } else
          true
      })
      .sortWith((x: MavenRelease, y: MavenRelease) =>
        x.pubDate.before(y.pubDate))
  }
}
