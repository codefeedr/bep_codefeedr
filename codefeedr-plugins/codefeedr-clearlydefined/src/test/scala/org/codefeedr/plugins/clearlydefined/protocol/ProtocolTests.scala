package org.codefeedr.plugins.clearlydefined.protocol

import org.scalatest.FunSuite
import org.codefeedr.plugins.clearlydefined.protocol.Protocol._

class ProtocolTests extends FunSuite {
  /** all full example classes */
  val cdDescribedUrls: CDDescribedUrls = CDDescribedUrls("registry", "version", "download")
  val cdDescribedHashes: CDDescribedHashes = CDDescribedHashes(Some("gitSha"), Some("sha1"), Some("sha256"))
  val cdDescribedToolScore: CDDescribedToolScore = CDDescribedToolScore(1, 2, 3)
  val cdDescribedSourceLocation: CDDescribedSourceLocation = CDDescribedSourceLocation("locationType", "provider", "namespace", "name", "revision",
    "url")
  val cdDescribedScore: CDDescribedScore = CDDescribedScore(4, 5, 6)
  val cdLicensedToolScore: CDLicensedToolScore = CDLicensedToolScore(11, 12, 13, 14, 15, 16)
  val cdlfCoreAttribution: CDLFCoreAttribution = CDLFCoreAttribution(1, Some(List("parties1", "parties2")))
  val cdlfCoreDiscovered: CDLFCoreDiscovered = CDLFCoreDiscovered(1, List("expressions1", "expressions2"))
  val cdlfCore: CDLFCore = CDLFCore(cdlfCoreAttribution, cdlfCoreDiscovered, 1)
  val cdLicensedFacets: CDLicensedFacets = CDLicensedFacets(cdlfCore)
  val cdLicensedScore: CDLicensedScore = CDLicensedScore(117, 118, 119, 120, 121, 122)
  val cdLicensed: CDLicensed = CDLicensed(Some("declared"), cdLicensedToolScore, cdLicensedFacets, cdLicensedScore)
  val cdCoordinates: CDCoordinates = CDCoordinates("type", "provider", "name", Some("namespace"), "revision")
  val cd_meta: CD_meta = CD_meta("schemaVersion", "2020-02-04T10:38:28.427Z")
  val cdScores: CDScores = CDScores(7, 8)
  val cdDescribed: CDDescribed = CDDescribed("releaseDate", cdDescribedUrls, Some("projectWebsite"), Some("issueTracker"),
    cdDescribedHashes, 1, List("tools1", "tools2"), cdDescribedToolScore, Some(cdDescribedSourceLocation),
    cdDescribedScore)

  val cdRelease: ClearlyDefinedRelease = ClearlyDefinedRelease(cdDescribed, cdLicensed, cdCoordinates, cd_meta, cdScores)

  /** all classes with potential None fields */
  val cdDescribedHashesEmpty: CDDescribedHashes = CDDescribedHashes(None, None, None)
  val cdDescribedEmpty: CDDescribed = CDDescribed("releaseDate", cdDescribedUrls, None, None, cdDescribedHashesEmpty, 1,
    List("tools1", "tools2"), cdDescribedToolScore, None, cdDescribedScore)
  val cdCoordinatesEmpty: CDCoordinates = CDCoordinates("type", "provider", "name", None, "revision")

  val cdReleaseEmpty: ClearlyDefinedRelease = ClearlyDefinedRelease(cdDescribedEmpty, cdLicensed, cdCoordinatesEmpty, cd_meta, cdScores)

  test("ClearlyDefinedReleasePojo convert success") {
    val pojo = ClearlyDefinedReleasePojo.fromClearlyDefinedRelease(cdRelease)

    // Assert various normal members and None members
    assert(pojo.described.releaseDate.equals("releaseDate"))
    assert(pojo.described.issueTracker.equals("issueTracker"))
    assert(pojo.licensed.score.spdx == 121)
    assert(pojo.coordinates.namespace.equals("namespace"))
    assert(pojo._meta.updated.toString.equals("2020-02-04 10:38:28.427"))
    assert(pojo.scores.effective == 7)
  }

  test("ClearlyDefinedReleasePojo convert with None fields success") {
    val pojo = ClearlyDefinedReleasePojo.fromClearlyDefinedRelease(cdReleaseEmpty)

    // Assert none fields
    assert(pojo.described.issueTracker == null)
    assert(pojo.described.projectWebsite == null)
    assert(pojo.described.sourceLocation == null)
    assert(pojo.described.hashes.gitSha == null)
    assert(pojo.described.hashes.sha1 == null)
    assert(pojo.described.hashes.sha256 == null)
    assert(pojo.coordinates.namespace == null)
  }

  test("CDDescribed convert success") {
    val pojo = CDDescribedPojo.fromCDDescribed(cdDescribed)

    // Assert fields
    assert(pojo.releaseDate.equals("releaseDate"))
    assert(pojo.projectWebsite.equals("projectWebsite"))
    assert(pojo.issueTracker.equals("issueTracker"))
    assert(pojo.files == 1)
    assert(pojo.tools.size==2)
    assert(pojo.tools.head.toolname=="tools1")
    assert(pojo.tools.last.toolname=="tools2")

    // Assert complex fields
    assert(pojo.urls.isInstanceOf[CDDescribedUrlsPojo])
    assert(pojo.hashes.isInstanceOf[CDDescribedHashesPojo])
    assert(pojo.toolScore.isInstanceOf[CDDescribedToolScorePojo])
    assert(pojo.sourceLocation.isInstanceOf[CDDescribedSourceLocationPojo])
    assert(pojo.score.isInstanceOf[CDDescribedScorePojo])
  }

  test("CDDescribed convert with none fields success") {
    val pojo = CDDescribedPojo.fromCDDescribed(cdDescribedEmpty)

    // Assert fields + None's
    assert(pojo.releaseDate.equals("releaseDate"))
    assert(pojo.projectWebsite == null)
    assert(pojo.issueTracker == null)
    assert(pojo.files == 1)
    assert(pojo.tools.head.toolname=="tools1")
    assert(pojo.tools.last.toolname=="tools2")
    assert(pojo.sourceLocation == null)
  }

  test("CDDescribedUrls convert success") {
    val pojo = CDDescribedUrlsPojo.fromCDDescribedUrls(cdDescribedUrls)

    // Assert fields
    assert(pojo.registry.equals("registry"))
    assert(pojo.version.equals("version"))
    assert(pojo.download.equals("download"))
  }

  test("CDDescribedHashes convert success") {
    val pojo = CDDescribedHashesPojo.fromCDDescribedHashes(cdDescribedHashes)

    // Assert fields
    assert(pojo.gitSha.equals("gitSha"))
    assert(pojo.sha1.equals("sha1"))
    assert(pojo.sha256.equals("sha256"))
  }

  test("CDDescribedHashes convert with None fields success") {
    val pojo = CDDescribedHashesPojo.fromCDDescribedHashes(cdDescribedHashesEmpty)

    // Assert fields
    assert(pojo.gitSha == null)
    assert(pojo.sha1 == null)
    assert(pojo.sha256 == null)
  }

  test("CDDescribedToolScore convert success") {
    val pojo = CDDescribedToolScorePojo.fromCDDescribedToolScore(cdDescribedToolScore)

    // Assert fields
    assert(pojo.total == 1)
    assert(pojo.date == 2)
    assert(pojo.source == 3)
  }

  test("CDDescribedSourceLocation convert success") {
    val pojo = CDDescribedSourceLocationPojo.fromCDDescribedSourceLocation(cdDescribedSourceLocation)

    // Assert fields
    assert(pojo.locationType.equals("locationType"))
    assert(pojo.provider.equals("provider"))
    assert(pojo.namespace.equals("namespace"))
    assert(pojo.name.equals("name"))
    assert(pojo.revision.equals("revision"))
    assert(pojo.url.equals("url"))
  }

  test("CDDescribedScore convert success") {
    val pojo = CDDescribedScorePojo.fromCDDescribedScore(cdDescribedScore)

    // Assert fields
    assert(pojo.total == 4)
    assert(pojo.date == 5)
    assert(pojo.source == 6)
  }

  test("CDLicensed convert success") {
    val pojo = CDLicensedPojo.fromCDLicensed(cdLicensed)

    // Assert fields
    assert(pojo.declared.equals("declared"))
    assert(pojo.toolScore.isInstanceOf[CDLicensedToolScorePojo])
    assert(pojo.facets.isInstanceOf[CDLicensedFacetsPojo])
    assert(pojo.score.isInstanceOf[CDLicensedScorePojo])
  }

  test("CDLicensedToolScore convert success") {
    val pojo = CDLicensedToolScorePojo.fromCDLicensedToolScore(cdLicensedToolScore)

    // Assert fields
    assert(pojo.total == 11)
    assert(pojo.declared == 12)
    assert(pojo.discovered == 13)
    assert(pojo.consistency == 14)
    assert(pojo.spdx == 15)
    assert(pojo.texts == 16)
  }

  test("CDLicensedFacets convert success") {
    val pojo = CDLicensedFacetsPojo.fromCDLicensedFacets(cdLicensedFacets)

    // Assert fields
    assert(pojo.core.isInstanceOf[CDLFCorePojo])
  }

  test("CDLFCore convert success") {
    val pojo = CDLFCorePojo.fromCDLFCore(cdlfCore)

    // Assert fields
    assert(pojo.attribution.isInstanceOf[CDLFCoreAttributionPojo])
    assert(pojo.discovered.isInstanceOf[CDLFCoreDiscoveredPojo])
    assert(pojo.files == 1)
  }

  test("CDLFCoreAttribution convert success") {
    val pojo = CDLFCoreAttributionPojo.fromCDLFCoreAttribution(cdlfCoreAttribution)

    // Assert fields
    assert(pojo.unknown == 1)
    assert(pojo.parties.head.equals("parties1"))
  }

  test("CDLFCoreDiscovered convert success") {
    val pojo = CDLFCoreDiscoveredPojo.fromCDLFCoreDiscovered(cdlfCoreDiscovered)

    // Assert fields
    assert(pojo.unknown == 1)
    assert(pojo.expressions.head.equals("expressions1"))
  }

  test("CDLicensedScore convert success") {
    val pojo = CDLicensedScorePojo.fromCDLicensedScore(cdLicensedScore)

    // Assert fields
    assert(pojo.total == 117)
    assert(pojo.declared == 118)
    assert(pojo.discovered == 119)
    assert(pojo.consistency == 120)
    assert(pojo.spdx == 121)
    assert(pojo.texts == 122)
  }

  test("CDCoordinates convert success") {
    val pojo = CDCoordinatesPojo.fromCDCoordinates(cdCoordinates)

    // Assert fields
    assert(pojo.`type`.equals("type"))
    assert(pojo.provider.equals("provider"))
    assert(pojo.name.equals("name"))
    assert(pojo.namespace.equals("namespace"))
    assert(pojo.revision.equals("revision"))
  }

  test("CDCoordinates convert with None fields success") {
    val pojo = CDCoordinatesPojo.fromCDCoordinates(cdCoordinatesEmpty)

    // Assert fields
    assert(pojo.`type`.equals("type"))
    assert(pojo.provider.equals("provider"))
    assert(pojo.name.equals("name"))
    assert(pojo.namespace == null)
    assert(pojo.revision.equals("revision"))
  }

  test("CD_meta convert success") {
    val pojo = CD_metaPojo.fromCD_meta(cd_meta)

    // Assert fields
    assert(pojo.schemaVersion.equals("schemaVersion"))
    assert(pojo.updated.toString.equals("2020-02-04 10:38:28.427"))
  }

  test("CDScores convert success") {
    val pojo = CDScoresPojo.fromCDScores(cdScores)

    // Assert fields
    assert(pojo.effective == 7)
    assert(pojo.tool == 8)
  }

}
