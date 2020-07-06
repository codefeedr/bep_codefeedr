package org.codefeedr.plugins.cargo.operators

import java.util.Date

import org.scalatest.FunSuite
import org.codefeedr.plugins.cargo.protocol.Protocol.CrateLinks
import org.codefeedr.plugins.cargo.resources.{CargoReleasesSnapshot, CrateSnapshot}
import spray.json._

class JsonParserTests extends FunSuite {

  val cargoReleaseSnapshot  :JsObject = CargoReleasesSnapshot.snapshot.parseJson.asJsObject()
  val crateSnapshot         :JsObject = CrateSnapshot.snapshot.parseJson.asJsObject()

  test("getNumCratesFromSummary success") {
    // Act
    val numCrates   :Option[Int]  = JsonParser.getNumCratesOrDownloadsFromSummary(cargoReleaseSnapshot, "num_crates")

    // Assert
    assert(numCrates.get == 32478)
  }

  test("getNumCratesFromSummary throws with wrong input") {
    // Act
    val numCrates   :Option[Int] = JsonParser.getNumCratesOrDownloadsFromSummary(crateSnapshot, "num_crates")

    // Assert
    assert(numCrates.isEmpty)
  }

  test("getNumDownloadsFromSummary success") {
    // Act
    val numDownloads   :Option[Int] = JsonParser.getNumCratesOrDownloadsFromSummary(cargoReleaseSnapshot, "num_downloads")

    // Assert
    assert(numDownloads.get == 1820298166)
  }

  test("getNumDownloadsFromSummary throws with wrong input") {
    // Act
    val numDownloads   :Option[Int] = JsonParser.getNumCratesOrDownloadsFromSummary(crateSnapshot, "num_downloads")

    // Assert
    assert(numDownloads.isEmpty)
  }

  test("getNewCratesFromSummary success") {
    // Act
    val newCrates   :Option[Vector[JsObject]] = JsonParser.getNewOrUpdatedCratesFromSummary(cargoReleaseSnapshot, "new_crates")

    assert(newCrates.get.size == 10)
  }

  test("getNewCratesFromSummary throws with wrong input") {
    // Act
    val newCrates   :Option[Vector[JsObject]] = JsonParser.getNewOrUpdatedCratesFromSummary(crateSnapshot, "new_crates")

    assert(newCrates.isEmpty)
  }

  test("getIntFieldFromCrate success") {
    // Arrange (first crate has id "dtd")
    val firstCrate = JsonParser.getNewOrUpdatedCratesFromSummary(cargoReleaseSnapshot, "new_crates").get(0)

    // Act
    val downloads: Int = JsonParser.getIntFieldFromCrate(firstCrate, "downloads").get

    assert(downloads.isInstanceOf[Int])
  }

  test("getIntFieldFromCrate fails with non-Int field") {
    // Act
    val name = JsonParser.getIntFieldFromCrate(cargoReleaseSnapshot, "name")

    // Assert that name is a None type
    assert(name.isEmpty)
  }

  test("getStringFieldFromCrate fails when json input is not a crates") {
    // Act
    val id :Option[String] = JsonParser.getStringFieldFromCrate(cargoReleaseSnapshot, "id")

    assert(id.isEmpty)
  }

  test("getStringFieldFromCrate success") {
    // Arrange (first crate has id "dtd")
    val firstCrate = JsonParser.getNewOrUpdatedCratesFromSummary(cargoReleaseSnapshot, "new_crates").get(0)

    // Act
    val id :Option[String] = JsonParser.getStringFieldFromCrate(firstCrate, "id")

    assert(id.get.equals("dtd"))
  }

  test("getDateFieldFromCrate success") {
    // Arrange
    val firstCrate = JsonParser.getNewOrUpdatedCratesFromSummary(cargoReleaseSnapshot, "new_crates").get(0)

    // Act
    val created_at: Option[Date] = JsonParser.getDateFieldFromCrate(firstCrate, "created_at")

    assert(created_at.isDefined)
  }

  test("getDateFieldFromCrate fails with wrong input") {
    // Arrange
    val firstCrate = JsonParser.getNewOrUpdatedCratesFromSummary(cargoReleaseSnapshot, "new_crates").get(0)

    // Act
    val created_at: Option[Date] = JsonParser.getDateFieldFromCrate(firstCrate, "id")

    assert(created_at.isEmpty)
  }

  test("getUpdatedCratesFromSummary success") {
    // Act
    val updatedCrates = JsonParser.getNewOrUpdatedCratesFromSummary(cargoReleaseSnapshot, "just_updated")

    // Assert
    assert(updatedCrates.isDefined)
  }

  test("getUpdatedCratesFromSummary fails with wrong input") {
    // Act
    val updatedCrates = JsonParser.getNewOrUpdatedCratesFromSummary(crateSnapshot, "just_updated")

    // Assert
    assert(updatedCrates.isEmpty)
  }

  test("parseCrateJsonToCrateObject success") {
    // Arrange
    val crateJson = crateSnapshot.fields("crate").asJsObject()

    // Act
    val crate = JsonParser.parseCrateJsonToCrateObject(crateJson)

    assert(crate.isDefined)
    assert(crate.get.id.isInstanceOf[String])
    assert(crate.get.versions.isInstanceOf[List[Int]])
    assert(crate.get.keywords.isInstanceOf[List[String]])
    assert(crate.get.categories.isInstanceOf[List[String]])
    assert(crate.get.created_at.isInstanceOf[Date])
    assert(crate.get.downloads.isInstanceOf[Int])
    assert(crate.get.recent_downloads.isDefined)
    assert(crate.get.max_version.isInstanceOf[String])
    assert(crate.get.description.isInstanceOf[String])
    assert(crate.get.homepage.isEmpty)
    assert(crate.get.documentation.isDefined)
    assert(crate.get.repository.isDefined)
    assert(crate.get.links.isInstanceOf[CrateLinks])
    assert(crate.get.exact_match.isInstanceOf[Boolean])
  }

  test("parseCrateJsonToCrateObject fails with wrong input") {
    // Act
    val crate = JsonParser.parseCrateJsonToCrateObject(crateSnapshot)

    // Assert
    assert(crate.isEmpty)
  }

  test("parseCrateVersionsJsonToCrateVersionsObject success") {
    // Arrange
    val versionsJson = crateSnapshot.fields("versions").asInstanceOf[JsArray].elements.head.asJsObject()

    // Act
    val version = JsonParser.parseCrateVersionsJsonToCrateVersionsObject(versionsJson)

    // assert
    assert(version.isDefined)
  }

  test("parseCrateVersionsJsonToCrateVersionsObject fails with wrong input") {
    // Act
    val version = JsonParser.parseCrateVersionsJsonToCrateVersionsObject(crateSnapshot)

    // Assert
    assert(version.isEmpty)
  }

  test("parseCrateLinksJsonToCrateLinksObject success") {
    // Arrange
    val crateLinksJson = crateSnapshot.fields("crate").asJsObject().fields("links").asJsObject()

    // Act
    val crateLinks = JsonParser.parseCrateLinksJsonToCrateLinksObject(crateLinksJson)

    // Assert
    assert(crateLinks.isDefined)
  }

  test("parseCrateLinksJsonToCrateLinksObject fails with wrong input") {
    // Act
    val crateLinks = JsonParser.parseCrateLinksJsonToCrateLinksObject(crateSnapshot)

    // Assert
    assert(crateLinks.isEmpty)
  }

  test("parseCrateVersionLinksJsonToCrateVersionLinksObject success") {
    // Arrange
    val crateVersionLinksJson = crateSnapshot.fields("versions").asInstanceOf[JsArray]
      .elements.head.asJsObject().fields("links").asJsObject()

    // Act
    val crateVersionLinks = JsonParser.parseCrateVersionLinksJsonToCrateVersionLinksObject(crateVersionLinksJson)

    // Assert
    assert(crateVersionLinks.isDefined)
  }

  test("parseCrateVersionLinksJsonToCrateVersionLinksObject fails with wrong input"){
    // Act
    val crateVersionLinks = JsonParser.parseCrateVersionLinksJsonToCrateVersionLinksObject(crateSnapshot)

    // Assert
    assert(crateVersionLinks.isEmpty)
  }

  test("parseCrateVersionPublishedByJsonToCrateVersionPublishedByObject success") {
    // Arrange
    val crateVersionPublishedByJson = crateSnapshot.fields("versions").asInstanceOf[JsArray]
      .elements.head.asJsObject().fields("published_by").asJsObject()

    // Act
    val crateVersionPublishedBy = JsonParser.parseCrateVersionPublishedByJsonToCrateVersionPublishedByObject(crateVersionPublishedByJson)

    // Assert
    assert(crateVersionPublishedBy.isDefined)
  }

  test("parseCrateVersionPublishedByJsonToCrateVersionPublishedByObject fails with wrong input") {
    // Act
    val crateVersionPublishedBy = JsonParser.parseCrateVersionPublishedByJsonToCrateVersionPublishedByObject(crateSnapshot)

    // Assert
    assert(crateVersionPublishedBy.isEmpty)
  }

  test("parseCrateKeywordsJsonToCrateKeywordsObject fails with wrong input") {
    // Act
    val crateKeywords = JsonParser.parseCrateKeywordsJsonToCrateKeywordsObject(crateSnapshot)

    // Assert
    assert(crateKeywords.isEmpty)
  }

  test("parseCrateCategoryJsonToCrateCategoryObject fails with wrong input") {
    // Act
    val crateKeywords = JsonParser.parseCrateCategoryJsonToCrateCategoryObject(crateSnapshot)

    // Assert
    assert(crateKeywords.isEmpty)
  }

  test("parseCrateJsonToCrateRelease fails with wrong input") {
    // Act
    val crateRelease = JsonParser.parseCrateJsonToCrateRelease(cargoReleaseSnapshot)

    // Assert
    assert(crateRelease.isEmpty)
  }

  test("getListOfIntsFieldFromCrate fails with wrong input") {
    // Act
    val listOfInts = JsonParser.getListOfIntsFieldFromCrate(crateSnapshot, "bogus")

    // Assert
    assert(listOfInts.isEmpty)
  }

  test("getListOfStringsFieldFromCrate fails with wrong input") {
    // Act
    val listOfStrings = JsonParser.getListOfStringsFieldFromCrate(crateSnapshot, "bogus")

    // Assert
    assert(listOfStrings.isEmpty)
  }

}
