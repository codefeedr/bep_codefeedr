package org.codefeedr.plugins.clearlydefined.operators

import org.codefeedr.plugins.clearlydefined.protocol.Protocol.ClearlyDefinedRelease
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.read
import org.scalatest.FunSuite
import org.codefeedr.plugins.clearlydefined.resources.ClearlyDefinedPackageSnapshot

class ClearlyDefinedReleasesSourceTests extends FunSuite{

  val cdrs = new ClearlyDefinedReleasesSource()

  test("getRSSAsString returns Option[String] and does not take too long") {
    // Arrange
    val startTime = System.currentTimeMillis()

    // Act
    val result = cdrs.getRSSAsString

    // Assert
    assert(result.isDefined)
    // Assert that the string is not a small returned error string
    assert(result.get.length > 100)
    // Assert not longer than 3 seconds for a single web request
    assert(System.currentTimeMillis() - startTime < 3000)
  }

  test("jsonRead success with snapshot") {
    // Arrange
    implicit val formats: DefaultFormats.type = DefaultFormats

    // Act
    val result = read[ClearlyDefinedRelease](ClearlyDefinedPackageSnapshot.snapshot)

    // Assert
    assert(result.coordinates.name == "sweetalert2")
  }

  // TODO discover reason for test failures
//  test("parseRSSString success with web request") {
//    // Arrange
//    implicit val formats = DefaultFormats
//    val stringInput = cdrs.getRSSAsString.get
//
//    // Act
//    val result = cdrs.parseRSSString(stringInput)
//
//    // Assert
//    assert(result(0).coordinates.`type`.isInstanceOf[String])
//  }
//
//  test("dateFormatParser works for cd._meta.updated field") {
//    // Arrange
//    implicit val formats = DefaultFormats
//    val stringInput = cdrs.getRSSAsString.get
//    val tenCDReleases = cdrs.parseRSSString(stringInput)
//
//    // Act
//    val dateTime = tenCDReleases(0)._meta.updated
//    val parsedDateTime = Protocol.getDate(dateTime)
//
//    // Assert
//    assert(parsedDateTime.isInstanceOf[Date])
//  }

  test("parseRSSString fails with wrong input") {
    // Arrange
    implicit val formats: DefaultFormats.type = DefaultFormats

    // Act
    val result = cdrs.parseRSSString("bogus string")

    // Assert
    assert(result.isEmpty)
  }

  test("waitPollingInterval waits for x+ seoncds") {
    // Assert
    val withSeconds = 1000
    val newCdrs = new ClearlyDefinedReleasesSource(ClearlyDefinedSourceConfig(withSeconds, -1, 4))
    val startTime = System.currentTimeMillis()

    // Act
    newCdrs.waitPollingInterval(5)
    val newTime = System.currentTimeMillis()

    // Assert
    assert(newTime - startTime >= 5000)
  }
}
