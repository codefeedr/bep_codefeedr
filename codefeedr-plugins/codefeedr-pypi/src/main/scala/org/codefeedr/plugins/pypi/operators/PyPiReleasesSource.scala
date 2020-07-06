package org.codefeedr.plugins.pypi.operators

import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.codefeedr.pipeline.{PluginReleasesSource, PluginSourceConfig}
import org.codefeedr.plugins.pypi.protocol.Protocol.PyPiRelease
import org.codefeedr.stages.utilities.{HttpRequester, RequestException}
import scalaj.http.Http

import scala.xml.XML

case class PyPiSourceConfig(pollingInterval: Int = 1000,
                            maxNumberOfRuns: Int = -1)
    extends PluginSourceConfig

class PyPiReleasesSource(config: PyPiSourceConfig = PyPiSourceConfig())
    extends PluginReleasesSource[PyPiRelease](config) {

  /** Format and URL of RSS Feed. */
  val dateFormat = "EEE, dd MMM yyyy HH:mm:ss ZZ"
  val url = "https://pypi.org/rss/updates.xml"

  /** Runs the source.
    *
    * @param ctx the source the context.
    */
  override def run(ctx: SourceFunction.SourceContext[PyPiRelease]): Unit = {
    val lock = ctx.getCheckpointLock

    /** While is running or #runs left. */
    while (isRunning && runsLeft != 0) {
      lock.synchronized { // Synchronize to the checkpoint lock.
        try {
          // Polls the RSS feed
          val rssAsString = getRSSAsString
          // Parses the received rss items
          val items: Seq[PyPiRelease] = parseRSSString(rssAsString)

          // Decrease the amount of runs left.
          decreaseRunsLeft()

          // Collect right items and update last item
          val validSortedItems = sortAndDropDuplicates(items)
          validSortedItems.foreach(x =>
            ctx.collectWithTimestamp(x, x.pubDate.getTime))

          // call parent run
          super.runPlugin(ctx, validSortedItems)
        } catch {
          case _: Throwable =>
        }
      }
    }
  }

  /**
    * Drops items that already have been collected and sorts them based on times
    * @param items Potential items to be collected
    * @return Valid sorted items
    */
  def sortAndDropDuplicates(items: Seq[PyPiRelease]): Seq[PyPiRelease] = {
    items
      .filter((x: PyPiRelease) => {
        if (lastItem.isDefined)
          lastItem.get.pubDate.before(x.pubDate) && lastItem.get.link != x.link
        else
          true
      })
      .sortWith((x: PyPiRelease, y: PyPiRelease) => x.pubDate.before(y.pubDate))
  }

  /**
    * Requests the RSS feed and returns its body as a string.
    * Will keep trying with increasing intervals if it doesn't succeed
    * @return Body of requested RSS feed
    */
  @throws[RequestException]
  def getRSSAsString: String = {
    new HttpRequester().retrieveResponse(Http(url)).body
  }

  /**
    * Parses a string that contains xml with RSS items
    * @param rssString XML string with RSS items
    * @return Sequence of RSS items
    */
  def parseRSSString(rssString: String): Seq[PyPiRelease] = {
    try {
      val xml = XML.loadString(rssString)
      val nodes = xml \\ "item"
      for (t <- nodes) yield xmlToPyPiRelease(t)
    } catch {
      // If the string cannot be parsed return an empty list
      case _: Throwable => Nil
    }
  }

  /**
    * Parses a xml node to a RSS item
    * @param node XML node
    * @return RSS item
    */
  def xmlToPyPiRelease(node: scala.xml.Node): PyPiRelease = {
    val title = (node \ "title").text
    val link = (node \ "link").text
    val description = (node \ "description").text

    val formatter = new SimpleDateFormat(dateFormat)
    val pubDate = formatter.parse((node \ "pubDate").text)

    PyPiRelease(title, link, description, pubDate)
  }
}
