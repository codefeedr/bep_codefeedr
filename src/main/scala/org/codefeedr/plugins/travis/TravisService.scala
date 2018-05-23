package org.codefeedr.plugins.travis

import java.time.LocalDateTime

import org.codefeedr.keymanager.{KeyManager, StaticKeyManager}
import org.codefeedr.utilities.Http
import org.json4s.{DefaultFormats, JValue}
import org.json4s.ext.JavaTimeSerializers
import org.json4s.jackson.JsonMethods.parse

//object TravisService {
//  def main(args: Array[String]): Unit = {
////    new TravisService().getTravisBuilds("owner", repoName = "branch").builds.foreach(x => println(x.commit.sha, x.number, x.started_at, x.state))
//    println(new TravisService().getBuild(
//      "joskuijpers",
//      "bep_codefeedr",
//      "develop",
//      "bb5cc7e5a19a84d71d627d202d44ea81598d9a68",
//      LocalDateTime.MIN))
//  }
//}

class TravisService(keyManager: KeyManager) extends Serializable {

  lazy implicit val formats = DefaultFormats ++ JavaTimeSerializers.all

  private val url = "https://api.travis-ci.org"

  def repoIsActive(slug: String): Boolean = {
    val responseBody = getTravisResource("/repo/" + slug)
    val isActive = parse(responseBody).extract[TravisRepository].active.getOrElse(false)
    isActive
  }

  def getTravisBuilds(owner: String, repoName: String, branch: String = "", offset: Int = 0, limit: Int = 25): Option[TravisBuilds] = {
    getTravisBuildsWithSlug(owner + "%2F" + repoName, branch, offset, limit)
  }

  def getTravisBuildsWithSlug(slug: String, branch: String = "", offset: Int = 0, limit: Int = 25): Option[TravisBuilds] = {

//    println("\tGetting travis builds " + offset + " until " + (offset + limit))

    val url = "/repo/" + slug + "/builds" +
      (if (branch.nonEmpty) "?branch.name=" + branch else "?") +
      "&sort_by=started_at:desc" +
//      "&state=created,started,failed,passed" +
      "&include=build.repository" +
      "&offset=" + offset +
      "&limit=" + limit

    val responseBody = getTravisResource(url)

    val json = parse(responseBody)

    val builds = extract[TravisBuilds](json)
    Some(builds)
  }

  def getBuild(buildID: Int): TravisBuild = {
    val url = "/build/" + buildID

    val responseBody = getTravisResource(url)
    val json =  parse(responseBody)
    val build = extract[TravisBuild](json)
    build
  }

  private def extract[A : Manifest](json: JValue): A = {
    try {
      json.extract[A]
    } catch {
      case _: Throwable =>
        throw CouldNotExctractException("Could not extract case class from JValue: " + json)
    }
  }

  private def getTravisResource(endpoint: String): String = {
    try {
      new Http()
        .getRequest(url + endpoint)
        .headers(getHeaders)
        .asString.body
    } catch {
      case _: Throwable =>
        throw CouldNotGetResourceException("Could not get the requested resource from: " + url + endpoint)
    }
  }

  private def getHeaders = {
    ("Travis-API-Version", "3") ::
      ("Authorization", "token " + keyManager.request("travis").get.value) ::
      Nil

  }
}

final case class CouldNotGetResourceException(private val message: String = "",
                                        private val cause: Throwable = None.orNull)
  extends Exception(message, cause)

final case class CouldNotExctractException(private val message: String = "",
                                        private val cause: Throwable = None.orNull)
  extends Exception(message, cause)
