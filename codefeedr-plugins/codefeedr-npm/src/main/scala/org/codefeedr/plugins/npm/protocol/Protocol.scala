package org.codefeedr.plugins.npm.protocol

import java.util.Date

import com.sksamuel.avro4s.AvroProp

/**
  * Contains all the case classes and POJO equivalent classes to represent a NPM package release
  *
  * @author Roald van der Heijden
  * Date: 2019-12-19 (YYYY-MM-DD)
  */
object Protocol {

  case class NpmRelease(
      name: String,
      @AvroProp("rowtime", "true") retrieveDate: Date) // using ingestion time

  case class NpmReleaseExt(name: String,
                           @AvroProp("rowtime", "true")
                           retrieveDate: Date,
                           project: NpmProject)

  case class NpmProject(_id: String,
                        _rev: Option[String],
                        name: String,
                        author: Option[PersonObject],
                        contributors: Option[List[PersonObject]],
                        description: Option[String],
                        homepage: Option[String],
                        keywords: Option[List[String]],
                        license: Option[String],
                        dependencies: Option[List[Dependency]],
                        maintainers: List[PersonObject],
                        readme: String,
                        readmeFilename: String,
                        bugs: Option[Bug],
                        bugString: Option[String],
                        repository: Option[Repository],
                        time: TimeObject)

  case class Dependency(packageName: String, version: String)

  case class PersonObject(name: String,
                          email: Option[String],
                          url: Option[String])

  case class Repository(`type`: String, url: String, directory: Option[String])

  case class Bug(url: Option[String], email: Option[String])

  case class TimeObject(created: String, modified: Option[String])

  // underneath is a POJO representation of all case classes mentioned above

  class NpmReleasePojo extends Serializable {
    var name: String = _
    var retrieveDate: Long = _
  }

  object NpmReleasePojo {
    def fromNpmRelease(release: NpmRelease): NpmReleasePojo = {
      val pojo = new NpmReleasePojo()
      pojo.name = release.name
      pojo.retrieveDate = release.retrieveDate.getTime
      pojo
    }
  }

  class NpmReleaseExtPojo extends Serializable {
    var name: String = _
    var retrieveDate: Long = _
    var project: NpmProjectPojo = _
  }

  object NpmReleaseExtPojo {
    def fromNpmReleaseExt(release: NpmReleaseExt): NpmReleaseExtPojo = {
      val pojo = new NpmReleaseExtPojo
      pojo.name = release.name
      pojo.retrieveDate = release.retrieveDate.getTime
      pojo.project = NpmProjectPojo.fromNpmProject(release.project)
      pojo
    }
  }

  class NpmProjectPojo extends Serializable {
    var _id: String = _
    var _rev: String = _
    var name: String = _
    var author: PersonObjectPojo = _
    var contributors: List[PersonObjectPojo] = _
    var description: String = _
    var homepage: String = _
    var keywords: List[NpmKeyWordPojo] = _
    var license: String = _
    var dependencies: List[DependencyPojo] = _
    var maintainers: List[PersonObjectPojo] = _
    var readme: String = _
    var readmeFilename: String = _
    var bugs: BugPojo = _
    var bugString: String = _
    var repository: RepositoryPojo = _
    var time: TimePojo = _
  }

  object NpmProjectPojo {
    def fromNpmProject(project: NpmProject): NpmProjectPojo = {
      val pojo = new NpmProjectPojo

      pojo._id = project._id
      pojo._rev = project._rev.orNull
      pojo.name = project.name
      if (project.author.isDefined) {
        pojo.author = PersonObjectPojo.fromPersonObject(project.author.get)
      }
      if (project.contributors.isDefined) {
        pojo.contributors = project.contributors.get
          .map(person => PersonObjectPojo.fromPersonObject(person))
      }
      pojo.description = project.description.orNull
      pojo.homepage = project.homepage.orNull
      if (project.keywords.isDefined) {
        pojo.keywords = project.keywords.get
          .map(keyword => NpmKeyWordPojo.fromKeywordAsString(keyword))
      }
      pojo.license = project.license.orNull
      if (project.dependencies.isDefined) {
        pojo.dependencies =
          project.dependencies.get.map(x => DependencyPojo.fromDependency(x))
      }
      pojo.maintainers = project.maintainers.map(
        person => PersonObjectPojo.fromPersonObject(person)
      )
      pojo.readme = project.readme
      pojo.readmeFilename = project.readmeFilename
      if (project.bugs.isDefined) {
        pojo.bugs = BugPojo.fromBug(project.bugs.get)
      }
      pojo.bugString = project.bugString.orNull
      if (project.repository.isDefined) {
        pojo.repository = RepositoryPojo.fromRepository(project.repository.get)
      }
      pojo.time = TimePojo.fromTime(project.time)

      pojo
    }
  }
  class NpmKeyWordPojo extends Serializable {
    var keyword: String = _
  }

  // added for the ability to register KeywordPojo as a streaming SQL table
  class NpmKeyWordPojoExt extends NpmKeyWordPojo {
    var id: String = _
  }

  object NpmKeyWordPojo {
    def fromKeywordAsString(keyword: String): NpmKeyWordPojo = {
      val pojo = new NpmKeyWordPojo()
      pojo.keyword = keyword
      pojo
    }
  }

  class DependencyPojo extends Serializable {
    var packageName: String = _
    var version: String = _
  }

  // added for the ability to register DependencyPojo as a streaming SQL table
  class DependencyPojoExt extends DependencyPojo {
    var id: String = _
  }

  object DependencyPojo {
    def fromDependency(dep: Dependency): DependencyPojo = {
      val pojo = new DependencyPojo()
      pojo.packageName = dep.packageName
      pojo.version = dep.version
      pojo
    }
  }

  class PersonObjectPojo extends Serializable {
    var name: String = _
    var email: String = _
    var url: String = _
  }

  // added for the ability to register PersonObjectPojo as a streaming SQL table
  class PersonObjectPojoExt extends PersonObjectPojo {
    var id: String = _
  }

  object PersonObjectPojo {
    def fromPersonObject(person: PersonObject): PersonObjectPojo = {
      val pojo = new PersonObjectPojo()
      pojo.name = person.name
      pojo.email = person.email.orNull
      pojo.url = person.url.orNull
      pojo
    }
  }

  class RepositoryPojo extends Serializable {
    var `type`: String = _
    var url: String = _
    var directory: String = _
  }

  // added for the ability to register RepositoryPojo as a streaming SQL table
  class RepositoryPojoExt extends RepositoryPojo {
    var id: String = _
  }

  object RepositoryPojo {
    def fromRepository(r: Repository): RepositoryPojo = {
      val pojo = new RepositoryPojo()
      pojo.`type` = r.`type`
      pojo.url = r.url
      pojo.directory = r.directory.orNull
      pojo
    }
  }

  class BugPojo extends Serializable {
    var url: String = _
    var email: String = _
  }

  // added for the ability to register BugPojo as a streaming SQL table
  class BugPojoExt extends BugPojo {
    var id: String = _
  }

  object BugPojo {
    def fromBug(b: Bug): BugPojo = {
      val pojo = new BugPojo()
      pojo.url = b.url.orNull
      pojo.email = b.email.orNull
      pojo
    }
  }

  class TimePojo extends Serializable {
    var created: String = _
    var modified: String = _
  }

  // added for the ability to register TimePojo as a streaming SQL table
  class TimePojoExt extends TimePojo {
    var id: String = _
  }

  object TimePojo {
    def fromTime(obj: TimeObject): TimePojo = {
      val pojo = new TimePojo()
      pojo.created = obj.created
      pojo.modified = obj.modified.orNull
      pojo
    }
  }

  override def toString: String = "Protocol companion object"
}
