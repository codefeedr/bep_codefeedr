package org.codefeedr.plugins.npm.protocol

import java.util.Date

import org.scalatest.FunSuite
import org.codefeedr.plugins.npm.protocol.Protocol.{Bug, BugPojo, Dependency, DependencyPojo, DependencyPojoExt, NpmProject, NpmProjectPojo, NpmRelease, NpmReleaseExt, NpmReleaseExtPojo, NpmReleasePojo, PersonObject, PersonObjectPojo, PersonObjectPojoExt, Repository, RepositoryPojo, RepositoryPojoExt, TimeObject, TimePojo, TimePojoExt}

/**
 * Class to test the creation of POJO for our SQL Service (since the Datastream[NPM Case Class] will not work
 * with field referencing.
 *
 * Some initial variables are declared and then each conversion method to convert a NPM Case Class into its relevant
 * NPM POJO is tested
 *
 * @author Roald van der Heijden
 * Date: 2019 - 12 - 19 (YYYY-MM-DD)
 */
class ProtocolTest extends FunSuite {

  // variables to test the creation of POJOs from their relevant case class counterparts
  val timeobj: TimeObject = TimeObject("2019-02-19T06:00:04.974Z", Some("2019-12-13T07:51:00.925Z"))
  val timeEmptyobj: TimeObject = TimeObject("2019-02-19T06:00:04.974Z", None)
  val bugobj0: Bug = Bug(Some("https:/github.com/nonexistentuser/projectname/issues"), Some("someUser@someDomain.com"))
  val bugobj1: Bug = Bug(Some("https://github.com/searchfe/ts2php/issues"), None)
  val bugobj2: Bug = Bug(None, Some("nospam@nourl.com"))
  val emptybugobj: Bug = Bug(None, None)
  val repoobj: Repository = Repository("git", "git+https://github.com/searchfe/ts2php.git", None)
  val emptyrepoobj: Repository = Repository("", "", None)
  val simplepersonobj: Option[String] = Some("Barney Rubble <b@rubble.com> (http://barnyrubble.tumblr.com/)")
  val personobj: PersonObject = PersonObject("cxtom", Some("cxtom2010@gmail.com"), None)
  val emptypersonobj: PersonObject = PersonObject("", None, None)
  val dependencyobj: Dependency = Dependency("semver", "^6.2.0")
  val bigProject: NpmProject = NpmProject("ts2php", Some("82-79c18b748261d1370bd45e0efa753721"), "ts2php", None,
    Some(List(PersonObject("cxtom", Some("cxtom2008@gmail.com"), None))), Some("TypeScript to PHP Transpiler"), Some("https://github.com/searchfe/ts2php#readme"), None, Some("MIT"),
    Some(List(Dependency("fs-extra", "^7.0.1"), Dependency("lodash", "^4.17.14"), Dependency("semver", "^6.2.0"))), List(PersonObject("cxtom", Some("cxtom2010@gmail.com"), None), PersonObject("meixg", Some("meixg@foxmail.com"), None)),
    "some story on how this project came to be",
    "indication where to find the above line", Some(Bug(Some("https://github.com/searchfe/ts2php/issues"), None)),
    None, Some(Repository("git", "git+https://github.com/searchfe/ts2php.git", None)), TimeObject("2019-02-19T06:00:04.974Z", Some("2019-12-13T07:51:00.925Z"))
  )
  val now = new Date(0)
  val npmrel: NpmRelease = NpmRelease("ts2php", now)
  val npmrele: NpmReleaseExt = NpmReleaseExt("ts2php", now, bigProject)

  val bigProject2: NpmProject = NpmProject("ts2php", Some("82-79c18b748261d1370bd45e0efa753721"), "ts2php", Some(personobj), // cxtom version
    Some(List(PersonObject("cxtom", Some("cxtom2008@gmail.com"), None))), Some("TypeScript to PHP Transpiler"), Some("https://github.com/searchfe/ts2php#readme"),
    Some(List("testing", "fullcoverage")),
      Some("MIT"),
    Some(List(Dependency("fs-extra", "^7.0.1"), Dependency("lodash", "^4.17.14"), Dependency("semver", "^6.2.0"))), List(PersonObject("cxtom", Some("cxtom2010@gmail.com"), None), PersonObject("meixg", Some("meixg@foxmail.com"), None)),
    "some story on how this project came to be",
    "indication where to find the above line", Some(Bug(Some("https://github.com/searchfe/ts2php/issues"), None)),
    None, Some(Repository("git", "git+https://github.com/searchfe/ts2php.git", None)), TimeObject("2019-02-19T06:00:04.974Z", Some("2019-12-13T07:51:00.925Z"))
  )


  // Start of tests


  test("POJO Test - NpmRelease POJO creation") {
    val pojo = NpmReleasePojo.fromNpmRelease(npmrel)
    // Assert
    assert(pojo.name == "ts2php")
    assert(pojo.retrieveDate == 0)
  }

  test("POJO Test - NpmReleaseExt POJO creation") {
    val result = NpmReleaseExtPojo.fromNpmReleaseExt(npmrele)
    // Assert
    assert(result.project.name == "ts2php")
    assert(result.retrieveDate == 0)
    assert(result.project.name == "ts2php")
    assert(result.project._id == "ts2php")
    assert(result.project._rev == "82-79c18b748261d1370bd45e0efa753721")
    assert(result.project.name == "ts2php")
    assert(result.project.author == null)
    assert(result.project.bugString == null)
    assert(result.project.readme == "some story on how this project came to be")
    assert(result.project.readmeFilename == "indication where to find the above line")
    assert(result.project.contributors.head.email == "cxtom2008@gmail.com")
    assert(result.project.dependencies.head.packageName == "fs-extra")
    assert(result.project.dependencies.last.packageName == "semver")
    assert(result.project.license == "MIT")
    assert(result.project.maintainers.head.name == "cxtom")
    assert(result.project.maintainers.last.name == "meixg")
    assert(result.project.description == "TypeScript to PHP Transpiler")
    assert(result.project.homepage == "https://github.com/searchfe/ts2php#readme")
    assert(result.project.keywords == null)
    assert(result.project.bugs.url == "https://github.com/searchfe/ts2php/issues")
    assert(result.project.bugString == null)
    assert(result.project.repository.url == "git+https://github.com/searchfe/ts2php.git")
    assert(result.project.time.modified == "2019-12-13T07:51:00.925Z")
  }

  test("POJO Test - NpmReleaseExt POJO creation - alternative paths") {
    val extendedRelease = NpmReleaseExt("ts2php", new Date(1), bigProject2)
    val result = NpmReleaseExtPojo.fromNpmReleaseExt(extendedRelease)
    // Assert
    assert(result.project.name == "ts2php")
    assert(result.retrieveDate == 1)
    assert(result.project.name == "ts2php")
    assert(result.project._id == "ts2php")
    assert(result.project._rev == "82-79c18b748261d1370bd45e0efa753721")
    assert(result.project.name == "ts2php")
    assert(result.project.author.name == "cxtom")
    assert(result.project.author.email == "cxtom2010@gmail.com")
    assert(result.project.author.url == null)
    assert(result.project.bugString == null)
    assert(result.project.readme == "some story on how this project came to be")
    assert(result.project.readmeFilename == "indication where to find the above line")
    assert(result.project.contributors.head.email == "cxtom2008@gmail.com")
    assert(result.project.dependencies.head.packageName == "fs-extra")
    assert(result.project.dependencies.last.packageName == "semver")
    assert(result.project.license == "MIT")
    assert(result.project.maintainers.head.name == "cxtom")
    assert(result.project.maintainers.last.name == "meixg")
    assert(result.project.description == "TypeScript to PHP Transpiler")
    assert(result.project.homepage == "https://github.com/searchfe/ts2php#readme")
    assert(result.project.keywords.size == 2)
    assert(result.project.keywords.head.keyword == "testing")
    assert(result.project.keywords.last.keyword == "fullcoverage")
    assert(result.project.bugs.url == "https://github.com/searchfe/ts2php/issues")
    assert(result.project.bugString == null)
    assert(result.project.repository.url == "git+https://github.com/searchfe/ts2php.git")
    assert(result.project.time.modified == "2019-12-13T07:51:00.925Z")
  }

  test("POJO Test - NpmProject POJO creation") {
    val result = NpmProjectPojo.fromNpmProject(bigProject)
    // Assert
    assert(result._id == "ts2php")
    assert(result._rev == "82-79c18b748261d1370bd45e0efa753721")
    assert(result.name == "ts2php")
    assert(result.author == null)
    assert(result.author == null)
    assert(result.bugString == null)
    assert(result.readme == "some story on how this project came to be")
    assert(result.readmeFilename == "indication where to find the above line")
    assert(result.contributors.head.email == "cxtom2008@gmail.com")
    assert(result.dependencies.head.packageName == "fs-extra")
    assert(result.dependencies.last.packageName == "semver")
    assert(result.license == "MIT")
    assert(result.maintainers.head.name == "cxtom")
    assert(result.maintainers.last.name == "meixg")
    assert(result.description == "TypeScript to PHP Transpiler")
    assert(result.homepage == "https://github.com/searchfe/ts2php#readme")
    assert(result.keywords == null)
    assert(result.bugs.url == "https://github.com/searchfe/ts2php/issues")
    assert(result.bugString == null)
    assert(result.repository.url == "git+https://github.com/searchfe/ts2php.git")
    assert(result.time.modified == "2019-12-13T07:51:00.925Z")
  }

  test("POJO Test - Npmproject Pojo creation - alternative paths") {
    // Arrange
    val alternativePathProject = Protocol.NpmProject("project_id", Some("_rev0.1"), "Harald", None, None, None, None, None, None, None,
      List(PersonObject("Roald", Some("roaldheijden@nospam.com"), Some("https://github.com/roaldvanderheijden"))),
      "readme: this is a short readme",
    "this is the link to the readme filename", None, None, None, TimeObject("2020-01-12T07:51:00.925Z", Some("2020-01-13T00:33:00.925Z")))
    // Act
    val result = NpmProjectPojo.fromNpmProject(alternativePathProject)
    // assert
    assert(result._id == "project_id")
    assert(result._rev == "_rev0.1")
    assert(result.name == "Harald")
    assert(result.author == null)
    assert(result.contributors == null)
    assert(result.description == null)
    assert(result.homepage == null)
    assert(result.keywords == null)
    assert(result.license == null)
    assert(result.dependencies == null)
    assert(result.maintainers.size == 1)
    assert(result.maintainers.head.name == "Roald")
    assert(result.maintainers.head.email == "roaldheijden@nospam.com")
    assert(result.maintainers.head.url == "https://github.com/roaldvanderheijden")
    assert(result.readme == "readme: this is a short readme")
    assert(result.readmeFilename == "this is the link to the readme filename")
    assert(result.contributors == null)
    assert(result.bugs == null)
    assert(result.bugString == null)
    assert(result.repository == null)
    assert(result.time.created == "2020-01-12T07:51:00.925Z")
    assert(result.time.modified == "2020-01-13T00:33:00.925Z")
  }

  test("POJO Test - Dependency POJO creation") {
    val result = DependencyPojo.fromDependency(dependencyobj)
    // Assert
    assert(result.packageName == "semver")
    assert(result.version == "^6.2.0")
  }

  test("POJO Test - Person POJO creation") {
    val result = PersonObjectPojo.fromPersonObject(personobj)
    // Assert
    assert(result.name == "cxtom")
    assert(result.email == "cxtom2010@gmail.com")
    assert(result.url == null)
  }

  test("POJO Test - empty Person POJO creation") {
    val result = PersonObjectPojo.fromPersonObject(emptypersonobj)
    // Assert
    assert(result.name == "")
    assert(result.email == null)
    assert(result.url == null)
  }

  test("POJO Test - partially filled repository POJO creation") {
    val result = RepositoryPojo.fromRepository(repoobj)
    // Assert
    assert(result.`type` == "git")
    assert(result.url == "git+https://github.com/searchfe/ts2php.git")
    assert(result.directory == null)
  }

  test("POJO Test - empty repository POJO creation") {
    val result = RepositoryPojo.fromRepository(emptyrepoobj)
    // Assert
    assert(result.`type` == "")
    assert(result.url == "")
    assert(result.directory == null)
  }

  test("POJO Test - fully filled BugObject Pojo creation") {
    val result = BugPojo.fromBug(bugobj0)
    // Assert
    assert(result.url == "https:/github.com/nonexistentuser/projectname/issues")
    assert(result.email == "someUser@someDomain.com")
  }

  test("POJO Test - partially filled BugObject POJO creation") {
    val result1 = BugPojo.fromBug(bugobj1)
    val result2 = BugPojo.fromBug(bugobj2)
    // Assert
    assert(result1.url == "https://github.com/searchfe/ts2php/issues")
    assert(result1.email == null)

    assert(result2.url == null)
    assert(result2.email == "nospam@nourl.com")
  }

  test("POJO Test - empty BugObject POJO creation") {
    val result = BugPojo.fromBug(emptybugobj)
    // Assert
    assert(result.url == null)
    assert(result.email == null)
  }

  test("POJO Test - filled TimeObject POJO creation") {
    val result = TimePojo.fromTime(timeobj)
    // Assert
    assert(result.created == "2019-02-19T06:00:04.974Z")
    assert(result.modified == "2019-12-13T07:51:00.925Z")
  }

  test("POJO Test - empty TimeObject POJO creation") {
    val result = TimePojo.fromTime(timeEmptyobj)
    // Assert
    assert(result.created == "2019-02-19T06:00:04.974Z")
    assert(result.modified == null)
  }

  test("POJO Test - PersonObjectPojoExt creation") {
    val result = new PersonObjectPojoExt()
    result.id = "ts2php"
    result.name = "Roald"
    result.email = "roaldheijden@nospam.com"
    result.url = "https://github.com"
    assert(result.id == "ts2php")
    assert(result.name == "Roald")
    assert(result.email == "roaldheijden@nospam.com")
    assert(result.url == "https://github.com")
  }

  test("POJO TEST - TimePojoExt creation") {
    val result = new TimePojoExt()
    result.id = "bslet"
    result.created = "2020-01-12"
    result.modified = "2020-01-13"
    assert(result.id == "bslet")
    assert(result.created == "2020-01-12")
    assert(result.modified == "2020-01-13")
  }

  test("POJO Test - DependencyPojoExt creation") {
    val result = new DependencyPojoExt()
    result.id = "upload.js"
    result.packageName = "semver"
    result.version = "6.0.3"
    assert(result.id == "upload.js")
    assert(result.packageName == "semver")
    assert(result.version == "6.0.3")
  }

  test("POJO Test - RepositoryPojoExt creation") {
    val result = new RepositoryPojoExt()
    result.id = "root"
    result.`type` = "git"
    result.url = "https://github.com/"
    result.directory = "roaldvanderheijden"
    assert(result.id == "root")
    assert(result.`type` == "git")
    assert(result.url == "https://github.com/")
    assert(result.directory == "roaldvanderheijden")
  }

  // Boilerplate tests (?) in an attempt to reach 100% coverage

  test("Unapply Test - NpmRelease case class") {
    assert(NpmRelease.unapply(npmrel).get == ("ts2php", new Date(0)))
  }

  test("Unapply Test - NpmReleaseExt case class") {
    val myExtendedRelease = NpmReleaseExt("someName", new Date(3), bigProject)
    // Assert
    assert(NpmReleaseExt.unapply(myExtendedRelease).get == ("someName", new Date(3), bigProject))
  }

  test("Unapply Test - NpmProject case class") {
    assert(NpmProject.unapply(bigProject2).get ==
      (
        "ts2php",
        Some("82-79c18b748261d1370bd45e0efa753721"),
        "ts2php",
        Some(personobj), // cxtom version
        Some(List(PersonObject("cxtom", Some("cxtom2008@gmail.com"), None))),
        Some("TypeScript to PHP Transpiler"),
        Some("https://github.com/searchfe/ts2php#readme"),
        Some(List("testing", "fullcoverage")),
        Some("MIT"),
        Some(List(Dependency("fs-extra", "^7.0.1"), Dependency("lodash", "^4.17.14"), Dependency("semver", "^6.2.0"))),
        List(PersonObject("cxtom", Some("cxtom2010@gmail.com"), None), PersonObject("meixg", Some("meixg@foxmail.com"), None)),
        "some story on how this project came to be",
        "indication where to find the above line",
        Some(Bug(Some("https://github.com/searchfe/ts2php/issues"), None)),
        None,
        Some(Repository("git", "git+https://github.com/searchfe/ts2php.git", None)),
        TimeObject("2019-02-19T06:00:04.974Z", Some("2019-12-13T07:51:00.925Z"))
      )
    )
  }

  test("Unapply Test - Dependency case class") {
    val depo = Dependency("somepackagename", "0.0.1")
    assert(Dependency.unapply(depo).get == (("somepackagename", "0.0.1")))
  }

  test("Unapply Test - PersonObject case class") {
    assert(PersonObject.unapply(personobj).get == ("cxtom", Some("cxtom2010@gmail.com"), None))
  }

  test("Unapply test - Repository case class") {
    assert(Repository.unapply(repoobj).get == ("git", "git+https://github.com/searchfe/ts2php.git", None))
  }

  test("Unapply Test - Bug case class") {
    val fullBug = Bug(Some("somewebpage.com/issues"), Some("user@domain.com"))
    assert(Bug.unapply(fullBug).get == (Some("somewebpage.com/issues"), Some("user@domain.com")))
  }

  test("Unapply Test - time case class") {
    assert(TimeObject.unapply(timeobj).get == ("2019-02-19T06:00:04.974Z", Some("2019-12-13T07:51:00.925Z")))
  }

  test("A basic or obscure test - that's the question - Testing toString & hashcode on Object Protocol") {
    assert(Protocol.toString() == "Protocol companion object")
  }
}