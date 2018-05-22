package org.codefeedr.plugins.travis

import java.time.LocalDateTime

import org.codefeedr.keymanager.{KeyManager, StaticKeyManager}
import org.codefeedr.utilities.Http
import org.json4s.DefaultFormats
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

class TravisService() {

  implicit val formats = DefaultFormats ++ JavaTimeSerializers.all

  private val url = "https://api.travis-ci.org"

  val keyManager: KeyManager = new StaticKeyManager(Map("travis" -> "su_9TrVO1Tbbti1UoG0Z_w"))

  def repoIsActive(slug: String): Boolean = {
    val responseBody = getTravisResource("/repo/" + slug)
    val isActive = parse(responseBody).extract[TravisRepository].active.getOrElse(false)
    isActive
  }

  def getTravisBuilds(owner: String, repoName: String, branch: String = "", offset: Int = 0, limit: Int = 25): TravisBuilds = {
    getTravisBuildsWithSlug(owner + "%2F" + repoName, branch, offset, limit)
  }

  def getTravisBuildsWithSlug(slug: String, branch: String = "", offset: Int = 0, limit: Int = 25): TravisBuilds = {

    println("\tGetting travis builds " + offset + " until " + (offset + limit))

    val url = "/repo/" + slug + "/builds" +
      (if (branch.nonEmpty) "?branch.name=" + branch else "?") +
      "&sort_by=started_at" +
//      "&state=created,started,failed,passed" +
      "&include=build.repository" +
      "&offset=" + offset +
      "&limit=" + limit

    val responseBody = getTravisResource(url)
    val json =  parse(responseBody)
    val builds = json.extract[TravisBuilds]
    builds
  }

//  def getBuild(owner: String, repoName: String, branch: String, pushCommitSha: String, afterDate: LocalDateTime, cachedBuilds: Option[TravisBuilds] = None): Option[TravisBuild] = {
//
//    var builds: TravisBuilds = cachedBuilds.orNull
//
//    do {
//      val offset = if (builds == null ) 0 else builds.`@pagination`.next.offset
//      println("offset", offset)
//      builds = getTravisBuilds(owner, repoName, branch, offset, limit = 5)
//      val buildIterator = builds.builds.iterator
//
//      while (buildIterator.hasNext) {
//        val x = buildIterator.next()
//        println(x.number)
//        if (x.commit.sha == pushCommitSha) {
//          return Some(x)
//        }
//        else if (x.started_at.isBefore(afterDate)) {
//          return None
//        }
//      }
//
//    } while (!builds.`@pagination`.is_last)
//
//    None
//  }

  def getBuild(buildID: Int): TravisBuild = {
    val url = "/build/" + buildID

    val responseBody = getTravisResource(url)
    val json =  parse(responseBody)
    val build = json.extract[TravisBuild]
    build
  }

  private def getTravisResource(endpoint: String): String = {
    new Http()
      .getRequest(url + endpoint)
      .headers(getHeaders())
      .asString.body
  }

  private def getHeaders() = {
    ("Travis-API-Version", "3") ::
      ("Authorization", "token " + keyManager.request("travis").get.value) ::
      Nil

  }
}
