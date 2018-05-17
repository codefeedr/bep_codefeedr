package org.codefeedr.plugins.travis

import org.codefeedr.utilities.Http
import org.json4s.jackson.JsonMethods.parse

class TravisService(token: String) {

  def repoIsActive(slug: String): Boolean = {
    val responseBody = new Http()
      .getRequest("https://api.travis-ci.org/repo/" + slug)
      .headers(
        ("Travis-API-Version", "3") ::
          ("Authorization", "token " + token) :: Nil)
      .asString.body

    println(parse(responseBody))

    val isActive = parse(responseBody).values.asInstanceOf[Map[Any, Boolean]]("active")
    isActive
  }

}
