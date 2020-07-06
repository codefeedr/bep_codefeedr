package org.codefeedr.plugins.cargo.protocol

import java.sql.Timestamp

import org.scalatest.FunSuite
import org.codefeedr.plugins.cargo.protocol.Protocol._

class ProtocolTests extends FunSuite {

  /** Instantiate object with full fields */
  val crateVersionFeatures: CrateVersionFeatures = CrateVersionFeatures()
  val crateVersionLinks: CrateVersionLinks = CrateVersionLinks("dependencies", "version_downloads", "authors")
  val crateVersionPublishedBy: CrateVersionPublishedBy = CrateVersionPublishedBy(1, "login", Some("name"), "avatar", "url")
  val crateVersion: CrateVersion = CrateVersion(1, "crate", "num", "dl_path", "readme_path",
    new Timestamp(0), new Timestamp(0), 100, crateVersionFeatures, yanked = true, "license", crateVersionLinks,
    Some(20), Some(crateVersionPublishedBy))
  val crateKeyword: CrateKeyword = CrateKeyword("id", "keyword", "created_at", 1)
  val crateCategory: CrateCategory =  CrateCategory("id", "category", "slug", "description", "created_at", 1)
  val crateLinks: CrateLinks = CrateLinks("version_downloads", Some("versions"), "owners",
    "owner_team", "owner_user", "reverse_dependencies")
  val crate: Crate = Crate("id", "name", new Timestamp(0), List(1, 2), List("keyword1", "keyword2"),
    List("category1", "category2"), new Timestamp(0), 100, Some(4), "max_version",
    "description", Some("homepage"), Some("documentation"), Some("repository"), crateLinks, exact_match = true)

  val crateRelease: CrateRelease = CrateRelease(crate, List(crateVersion, crateVersion), List(crateKeyword, crateKeyword),
    List(crateCategory, crateCategory))

  /** Instantiate object with empty fields */
  val crateVersionPublishedByEmpty: CrateVersionPublishedBy = CrateVersionPublishedBy(1, "login", None, "avatar", "url")
  val crateLinksEmpty: CrateLinks = CrateLinks("version_downloads", None, "owners",
    "owner_team", "owner_user", "reverse_dependencies")
  val crateEmpty: Crate = Crate("id", "name", new Timestamp(0), List(1, 2), List("keyword1", "keyword2"),
    List("category1", "category2"), new Timestamp(0), 100, None, "max_version",
    "description", None, None, None, crateLinksEmpty, exact_match = true)
  val crateVersionEmpty: CrateVersion = CrateVersion(1, "crate", "num", "dl_path", "readme_path",
    new Timestamp(0), new Timestamp(0), 100, crateVersionFeatures, yanked = true, "license", crateVersionLinks,
    None, None)

  val crateReleaseEmpty: CrateRelease = CrateRelease(crateEmpty, List(crateVersionEmpty, crateVersionEmpty),
    List(crateKeyword, crateKeyword), List(crateCategory, crateCategory))

  test("CrateReleasePojo convert with all fields success") {
    val pojo = CrateReleasePojo.fromCrateRelease(crateRelease)

    // Assert certain fields, e.g. Option[Int] crate.recent_downloads
    assert(pojo.crate.recent_downloads == 4)
    assert(pojo.crate.links.versions.equals("versions"))
    assert(pojo.versions.head.published_by.login.equals("login"))
    assert(pojo.keywords.head.keyword.equals("keyword"))
    assert(pojo.categories.head.category.equals("category"))
  }

  test("CrateReleasePojo convert with None fields success") {
    val pojo = CrateReleasePojo.fromCrateRelease(crateReleaseEmpty)

    // Assert certain None fields
    assert(pojo.crate.recent_downloads == 0)
    assert(pojo.crate.links.versions == null)
    assert(pojo.versions.head.published_by == null)
    // Assert a regular field
    assert(pojo.crate.name.equals("name"))
  }

  test("CratePojo convert with all fields success") {
    val pojo = CratePojo.fromCrate(crate)

    // Assert certain fields
    assert(pojo.id.equals("id"))
    assert(pojo.versions.head == 1)
    assert(pojo.repository.equals("repository"))
  }

  test("CratePojo convert with None fields success") {
    val pojo = CratePojo.fromCrate(crateEmpty)

    // Assert certain None fields
    assert(pojo.recent_downloads == 0)
    assert(pojo.homepage == null)
    assert(pojo.documentation == null)
    assert(pojo.repository == null)
    // Assert a regular field
    assert(pojo.exact_match)
  }

  test("CrateLinks convert with all fields success") {
    val pojo = CrateLinksPojo.fromCrateLinks(crateLinks)

    // Assert certain fields
    assert(pojo.version_downloads.equals("version_downloads"))
    assert(pojo.versions.equals("versions"))
    assert(pojo.owner_user.equals("owner_user"))
  }

  test("CrateLinks convert with None fields success") {
    val pojo = CrateLinksPojo.fromCrateLinks(crateLinksEmpty)

    // Assert None field
    assert(pojo.versions == null)
    // Assert regular field
    assert(pojo.owner_team.equals("owner_team"))
  }

  test("CrateVersion convert with all fields success") {
    val pojo = CrateVersionPojo.fromCrateVersion(crateVersion)

    // Assert certain fields
    assert(pojo.id == 1)
    assert(pojo.yanked)
    assert(pojo.license.equals("license"))
  }

  test("CrateVersionFeatures convert success") {
    val pojo = CrateVersionFeaturesPojo.fromCrateVersionFeatures()

    // Assert type correct
    assert(pojo.isInstanceOf[CrateVersionFeaturesPojo])
  }

  test("CrateVersionLinks with all fields success") {
    val pojo = CrateVersionLinksPojo.fromCrateVersionLinks(crateVersionLinks)

    // Assert certain fields
    assert(pojo.dependencies.equals("dependencies"))
    assert(pojo.version_downloads.equals("version_downloads"))
    assert(pojo.authors.equals("authors"))
  }

  test("CrateVersionPublishedBy with all fields success") {
    val pojo = CrateVersionPublishedByPojo.fromCrateVersionPublishedBy(crateVersionPublishedBy)

    // Assert certain fields
    assert(pojo.id == 1)
    assert(pojo.login.equals("login"))
    assert(pojo.name.equals("name"))
    assert(pojo.avatar.equals("avatar"))
    assert(pojo.url.equals("url"))
  }

  test("CrateVersionPublishedBy with None fields") {
    val pojo = CrateVersionPublishedByPojo.fromCrateVersionPublishedBy(crateVersionPublishedByEmpty)

    // Assert None field
    assert(pojo.name == null)
  }

  test("CrateKeyword with all fields success") {
    val pojo = CrateKeywordPojo.fromCrateKeyword(crateKeyword)

    // Assert fields
    assert(pojo.id.equals("id"))
    assert(pojo.keyword.equals("keyword"))
    assert(pojo.created_at.equals("created_at"))
    assert(pojo.crates_cnt == 1)
  }

  test("CrateCategory with all fields success") {
    val pojo = CrateCategoryPojo.fromCrateCategory(crateCategory)

    // Assert fields
    assert(pojo.id.equals("id"))
    assert(pojo.category.equals("category"))
    assert(pojo.slug.equals("slug"))
    assert(pojo.description.equals("description"))
    assert(pojo.created_at.equals("created_at"))
    assert(pojo.crates_cnt == 1)
  }

}
