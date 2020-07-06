package org.codefeedr.plugins.npm.util

import java.util.Date

import org.apache.logging.log4j.scala.Logging
import org.codefeedr.stages.utilities.HttpRequester
import org.json4s.JsonAST.{JNothing, JObject, JString}
import org.json4s.ext.JavaTimeSerializers
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.Serialization.read
import org.json4s.{DefaultFormats, Formats, JValue}
import org.codefeedr.plugins.npm.protocol.Protocol
import org.codefeedr.plugins.npm.protocol.Protocol.{
  Dependency,
  NpmProject,
  NpmReleaseExt,
  PersonObject,
  TimeObject
}
import scalaj.http.Http

/**
  * Services to retrieve a project from the NPM API registry.
  *
  * @author Roald van der Heijden
  * Date: 2019-12-01 (YYYY-MM-DD)
  */
object NpmService extends Logging with Serializable {

  var timeout = 8

  /**
    * the API url to retrieve projects from.
    */
  val url_packageInfo = "http://registry.npmjs.com/"

  /**
    * Retrieves a Npm project, resulting in the Protocol case class with information filled in where possible.
    *
    * @param projectName the name of the project.
    * @return an optional NpmProject.
    */
  def getProject(projectName: String): Option[NpmReleaseExt] = {
    for {
      jsonString <- createJsonStringFor(url_packageInfo, projectName)
      npmProject <- convertProjectFrom(jsonString)
    } yield buildNpmReleaseExtUsing(projectName, jsonString, npmProject)
  }

  /**
    * Creates a JSON String for this project
    *
    * @param projectName the project for which to get the information and create the JSON String
    * @return the JSON in Option[String] (so None, if something went wrong and also None when UNPUBLISHED!!)
    */
  def createJsonStringFor(
      updateStreamBaseURL: String,
      projectName: String
  ): Option[String] = {
    val jsonString: Option[String] =
      getProjectRaw(updateStreamBaseURL, projectName, timeout)
    if (jsonString.isEmpty || jsonString.get == """{"error":"Not found"}""") {
      logger.error(s"Couldn't retrieve npm project with name $projectName.")
      return None
    }
    // check if UNPUBLISHED, if so => err msg/None, else just the jsonString
    val json = parse(jsonString.get)
    (json \ "time") \ "unpublished" match {
      case JNothing => jsonString
      case _ =>
        logger.error(
          s"skipping UNPUBLISHED state of project with name $projectName."
        )
        None
    }
  }

  /**
    * Creates a NpmProject from given JSON String while taking care of some error handling as well
    * @param json the JSON String to parse
    * @return None if something went wrong, or Some[NpmProject]
    */
  def convertProjectFrom(json: String): Option[NpmProject] = {
    implicit val formats: Formats =
      new DefaultFormats {} ++ JavaTimeSerializers.all
    val myProject =
      try {
        Some(read[Protocol.NpmProject](json))
      } catch {
        case _: Throwable => None
      }
    myProject
  }

  /**
    * Builds an extended Release from given name, JSON String and NpmProject.
    * @param projectName the name of the project we want to build an extended release for
    * @param jsonString the JSON string belonging to this projectName
    * @param project the NPMproject extracted from given JSON String
    * @return an NpmReleaseExt with all required details filled in
    */
  def buildNpmReleaseExtUsing(
      projectName: String,
      jsonString: String,
      project: NpmProject
  ): NpmReleaseExt = {
    val json = parse(jsonString)
    // STEP 1: Now set the time right (find the created / modified field and update the time)
    val myTime = extractTimeFrom(json)
    // STEP 2 : Now lookup the dependencies
    val myDependencies = extractDependenciesFrom(json)
    // STEP 3: Then lookup the author
    val myAuthor = extractAuthorFrom(json)
    // STEP 4: Update the Case Class with the results of time & dependencies
    NpmReleaseExt(
      projectName,
      new Date(),
      project.copy(
        time = myTime,
        author = myAuthor,
        dependencies = Some(myDependencies)
      )
    )
  }

  /**
    * Parses the time for a given project
    * @param json the JValue to parse the time from
    * @return a case class TimeObject with relevant details filled in
    */
  def extractTimeFrom(json: JValue): TimeObject = {
    // find the first creation time
    val createdField = (json \ "time") \ "created" match {
      case JString(s: String) => s
      case _                  => "unknown"
    }
    // find the latest modification time
    val modifiedField = (json \ "time") \ "modified" match {
      case JString(s: String) => Some(s)
      case _                  => None
    }
    TimeObject(createdField, modifiedField)
  }

  /**
    * Finds the latest version string of a NPM Project
    * @param json the JSON object to look for the latest version in
    * @return a String denoting the latest version on success or "-1" on failure
    */
  def findLatestVersionNr(json: JValue): String = {
    (json \ "dist-tags") \ "latest" match {
      case JString(x) => x
      case _          => "-1"
    }
  }

  /**
    * Parses the dependencies for the latest version of a given project, if the field "latest" exists within the JSON
    * @param json the JValue from which we glean the List[DependencyObject]
    * @return the list with Dependencies or Nil if something went wrong
    */
  def extractDependenciesFrom(json: JValue): List[Dependency] = {
    // first look up the latest version number
    val latestVersionNr = findLatestVersionNr(json)
    // then get me that version object and look up the dependencies field
    val dependenciesList =
      ((json \ "versions") \ latestVersionNr) \ "dependencies" match {
        case JObject(lijstje) => lijstje
        case _                => Nil
      }
    // EXHAUSTIVE match using Option, then flatMap to get the correct type back!
    dependenciesList.flatMap(tupleElem =>
      tupleElem match {
        case (name, JString(version)) => Some(Dependency(name, version))
        case _                        => None
    })
  }

  /**
    * Tries to parse an author for a given project. Will look in the root children for a complex author object,
    * or in the field of the latest version if the author cannot be parsed from there.
    * @param json the JValue from which we glean complex PersonObject
    * @return Some[PersonObject] on success, None on failure
    */
  def extractAuthorFrom(json: JValue): Option[PersonObject] = {
    val authorField1 = json \ "author"
    val result1 = convertAuthorFrom(authorField1)
    if (result1.isDefined) result1
    else {
      val latestVersionNr = findLatestVersionNr(json)
      val authorField2 = ((json \ "versions") \ latestVersionNr) \ "author"
      convertAuthorFrom(authorField2)
    }
  }

  /**
    * converts given JSON into Option[PersonObject]
    * @param jsonAuthorField the JSON to convert
    * @return Some(...) on success, None on failure
    */
  def convertAuthorFrom(jsonAuthorField: JValue): Option[PersonObject] = {
    jsonAuthorField match {
      case JObject(_) =>
        implicit val formats: Formats =
          new DefaultFormats {} ++ JavaTimeSerializers.all
        jsonAuthorField.extractOpt[PersonObject]
      case _ => None
    }
  }

  /**
    * Returns a project as a raw string.
    *
    * @param endpoint the end_point to do the request.
    *
    * @return an optional JSON String.
    */
  def getProjectRaw(
      base_url: String,
      endpoint: String,
      maxTimeOutInsec: Int
  ): Option[String] = {
    val response =
      try {
        val request = Http(base_url + endpoint).headers(withConfiguredHeaders)
        new HttpRequester(maxTimeOutInsec).retrieveResponse(request)
      } catch {
        case _: Throwable => return None
      }
    Some(response.body)
  }

  /**
    * Add a user-agent with contact details.
    */
  def withConfiguredHeaders: List[(String, String)] = {
    (
      "User-Agent",
      "CodeFeedr-Npm/1.0 Contact: zonneveld.noordwijk@gmail.com"
    ) :: Nil
  }

  override def toString = "NpmService Companion Object"
}
