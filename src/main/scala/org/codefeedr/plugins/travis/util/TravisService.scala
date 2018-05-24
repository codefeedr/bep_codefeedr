package org.codefeedr.plugins.travis.util

import org.codefeedr.keymanager.KeyManager
import org.codefeedr.plugins.travis.TravisProtocol.{TravisBuild, TravisBuilds, TravisRepository}
import org.codefeedr.plugins.travis.util.TravisExceptions.{CouldNotExtractException, CouldNotGetResourceException}
import org.codefeedr.utilities.Http
import org.json4s.ext.JavaTimeSerializers
import org.json4s.jackson.JsonMethods.parse
import org.json4s.{DefaultFormats, Formats, JValue}

class TravisService(keyManager: KeyManager) extends Serializable {

  lazy implicit val formats: Formats = DefaultFormats ++ JavaTimeSerializers.all

  private val url = "https://api.travis-ci.org"

  /**
    * Returns a function that checks if a given repo is active on Travis.
    * @return True if it is active, false otherwise
    */
  def repoIsActiveFilter: String => Boolean = {
    val filter: String => Boolean = slug => {
      val responseBody = getTravisResource("/repo/" + slug.replace("/", "%2F"))
      val isActive = try {
        val active = parse(responseBody).extract[TravisRepository].active.getOrElse(false)
        active
      } catch {
        case e: org.json4s.MappingException =>
          false
      }
      isActive
    }
    filter
  }

  /**
    * Gives a specific page with build information of a give repository on a given branch.
    * @param owner Name of the owner of the repository
    * @param repoName Name of the repository
    * @param branch Name of the branch
    * @param offset How many builds to skip
    * @param limit How many builds to get
    * @return Page with Travis builds
    */
  def getTravisBuilds(owner: String, repoName: String, branch: String = "", offset: Int = 0, limit: Int = 25): TravisBuilds = {
    getTravisBuildsWithSlug(owner + "%2F" + repoName, branch, offset, limit)
  }

  /**
    * Gives a specific page with build information of a give repository on a given branch.
    * @param slug Slug of the repository
    * @param branch Name of the branch
    * @param offset How many builds to skip
    * @param limit How many builds to get
    * @return Page with Travis builds
    */
  def getTravisBuildsWithSlug(slug: String, branch: String = "", offset: Int = 0, limit: Int = 25): TravisBuilds = {

//    println("\tGetting travis builds " + offset + " until " + (offset + limit))

    val url = "/repo/" + slug + "/builds" +
      (if (branch.nonEmpty) "?branch.name=" + branch else "?") +
      "&sort_by=started_at:desc" +
//      "&state=created,started,failed,passed" +
      "&include=build.repository" +
      "&offset=" + offset +
      "&limit=" + limit

    val responseBody = getTravisResource(url)

    val json = try {
      parse(responseBody)
    } catch {
      case e: Throwable =>
        println(responseBody)
        throw e
    }

    val builds = extract[TravisBuilds](json)
    builds
  }

  /**
    * Requests a build from Travis with a specific id
    * @param buildID Id of the build
    * @return Travis build
    */
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
        throw CouldNotExtractException("Could not extract case class from JValue: " + json)
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


