package org.codefeedr.plugins.travis

import org.codefeedr.keymanager.{KeyManager, StaticKeyManager}
import org.codefeedr.utilities.Http
import org.json4s.DefaultFormats
import org.json4s.ext.JavaTimeSerializers
import org.json4s.jackson.JsonMethods.parse

object TravisService {
  def main(args: Array[String]): Unit = {
    new TravisService().getTravisBuilds("joskuijpers%2Fbep_codefeedr").builds.foreach(x => println(x.commit.sha, x.number, x.started_at, x.state))
  }
}

class TravisService() {

  implicit val formats = DefaultFormats ++ JavaTimeSerializers.all

  private val url = "https://api.travis-ci.org"

  val keyManager: KeyManager = new StaticKeyManager(Map("travis" -> "su_9TrVO1Tbbti1UoG0Z_w"))

  def repoIsActive(slug: String): Boolean = {
    val responseBody = getTravisResource("/repo/" + slug)
    val isActive = parse(responseBody).extract[TravisRepository].active.getOrElse(false)
    isActive
  }

  def getTravisBuilds(slug: String): TravisBuilds = {
    val responseBody = getTravisResource("/repo/" + slug + "/builds?sort_by=started_at:desc&state=started,failed,passed")
    val json =  parse(responseBody)
    val builds = json.extract[TravisBuilds]
    builds
  }

  def getTravisBuilds(slug: String, branch: String): TravisBuilds = {
    val responseBody = getTravisResource("/repo/" + slug + "/builds?branch.name=" + branch + "&sort_by=started_at&state=started,failed,passed")
    val json =  parse(responseBody)
    val builds = json.extract[TravisBuilds]
    builds
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
