package org.codefeedr.plugins.maven.operators

import org.scalatest.FunSuite

class MavenReleasesSourceTest extends FunSuite{

  val mavenReleasesSource = new MavenReleasesSource(MavenSourceConfig(500, -1, 4))

  test("parseRSSString bad"){
    val res = mavenReleasesSource.parseRSSString("wrong string")

    assert(res == Nil)
  }

  test("waitPollingInterval"){
    val startTime = System.currentTimeMillis()
    mavenReleasesSource.waitPollingInterval()
    val endTime = System.currentTimeMillis()

    assert(endTime - startTime >= 500)
  }

  test("defaultValuesTest"){
    val config = MavenSourceConfig()

    assert(config.maxNumberOfRuns == -1)
    assert(config.pollingInterval == 60000)
  }

}
