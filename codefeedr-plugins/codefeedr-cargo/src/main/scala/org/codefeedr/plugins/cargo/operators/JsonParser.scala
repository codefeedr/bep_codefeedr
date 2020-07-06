package org.codefeedr.plugins.cargo.operators

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.codefeedr.plugins.cargo.protocol.Protocol._
import spray.json.{JsArray, JsBoolean, JsNumber, JsObject, JsString, JsValue}

object JsonParser {
  //TODO, The last 3 S's might create bugs
  val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX")

  /**
    * Gets the total number of crates in the Cargo package source currently available
    * @param jsObject body of the Crate.io summary page parsed into JsObject
    * @param field string which denotes either 'num_crates' or 'num_downloads' to retrieve the corresponding value
    * @return Number of crates
    */
  def getNumCratesOrDownloadsFromSummary(jsObject: JsObject,
                                         field: String): Option[Int] = {
    try {
      val numCrates: Option[JsValue] = jsObject.fields.get(field)

      Some(
        numCrates.get
          .asInstanceOf[JsNumber]
          .value
          .toInt)
    } catch {
      case _: Throwable => None
    }
  }

  /**
    * Retrieves the ten most recently updated or brand new Crates from the summary page
    * @param jsObject Summary page parsed into a JsObject (crates.io/api/v1/summary)
    * @param field "new_crates" or "just_updated" for the type of crates to retrieve
    * @return A Vector consisting of 10 JsObjects which are the most recently updated or new crates
    */
  def getNewOrUpdatedCratesFromSummary(
      jsObject: JsObject,
      field: String): Option[Vector[JsObject]] = {
    try {
      val updatedCrates: Option[JsValue] = jsObject.fields.get(field)

      Some(
        updatedCrates.get
          .asInstanceOf[JsArray]
          .elements
          .distinct
          .map(x => x.asJsObject()))
    } catch {
      case _: Throwable => None
    }
  }

  /**
    * Method for parsing the CrateRelease Json into a CrateRelease object
    * Can be seen as the 'parent' method of this class
    * @param jsObject object to be parsed
    * @return
    */
  def parseCrateJsonToCrateRelease(jsObject: JsObject): Option[CrateRelease] = {
    try {
      val crate: JsObject = jsObject.fields("crate").asJsObject()
      val versions: List[JsValue] =
        jsObject.fields("versions").asInstanceOf[JsArray].elements.toList
      val keywords: List[JsValue] =
        jsObject.fields("keywords").asInstanceOf[JsArray].elements.toList
      val categories: List[JsValue] =
        jsObject.fields("categories").asInstanceOf[JsArray].elements.toList

      val crateObject: Crate = this.parseCrateJsonToCrateObject(crate).get
      val versionsObject: List[CrateVersion] = versions.map({ x =>
        this.parseCrateVersionsJsonToCrateVersionsObject(x.asJsObject).get
      })
      val keywordsObject: List[CrateKeyword] = keywords.map(
        x =>
          this
            .parseCrateKeywordsJsonToCrateKeywordsObject(
              x.asInstanceOf[JsObject])
            .get)
      val categoriesObject: List[CrateCategory] = categories.map(
        x =>
          this
            .parseCrateCategoryJsonToCrateCategoryObject(
              x.asInstanceOf[JsObject])
            .get)

      Some(
        CrateRelease(crateObject,
                     versionsObject,
                     keywordsObject,
                     categoriesObject))
    } catch {
      case e: Throwable =>
        printf(
          "\nparsing from Json to CrateRelease failed with info: " + e.toString)
        None
    }
  }

  /**
    * Method for parsing the Crate field in a CrateRelease object
    * @param jsObject object to be parsed
    * @return
    */
  def parseCrateJsonToCrateObject(jsObject: JsObject): Option[Crate] = {
    try {
      val id: String = this.getStringFieldFromCrate(jsObject, "id").get
      val name: String = this.getStringFieldFromCrate(jsObject, "name").get
      val updated_at: Timestamp =
        this.getDateFieldFromCrate(jsObject, "updated_at").get
      val versions: List[Int] =
        this.getListOfIntsFieldFromCrate(jsObject, "versions").get
      val keywords: List[String] =
        this.getListOfStringsFieldFromCrate(jsObject, "keywords").get
      val categories: List[String] =
        this.getListOfStringsFieldFromCrate(jsObject, "categories").get
      val created_at: Timestamp =
        this.getDateFieldFromCrate(jsObject, "created_at").get
      val downloads: Int = this.getIntFieldFromCrate(jsObject, "downloads").get
      val recent_downloads: Option[Int] =
        this.getIntFieldFromCrate(jsObject, "recent_downloads")
      val max_version: String =
        this.getStringFieldFromCrate(jsObject, "max_version").get
      val description: String =
        this.getStringFieldFromCrate(jsObject, "description").get
      val homepage: Option[String] =
        this.getStringFieldFromCrate(jsObject, "homepage")
      val documentation: Option[String] =
        this.getStringFieldFromCrate(jsObject, "documentation")
      val repository: Option[String] =
        this.getStringFieldFromCrate(jsObject, "repository")
      val links: CrateLinks =
        this
          .parseCrateLinksJsonToCrateLinksObject(
            jsObject.fields("links").asJsObject())
          .get
      val exact_match: Boolean =
        this.getBoolFieldFromCrate(jsObject, "exact_match").get

      Some(
        Crate(
          id,
          name,
          updated_at,
          versions,
          keywords,
          categories,
          created_at,
          downloads,
          recent_downloads,
          max_version,
          description,
          homepage,
          documentation,
          repository,
          links,
          exact_match
        )
      )
    } catch {
      case e: Throwable =>
        printf("\nparsing from Json to Crate failed with info: " + e.toString)
        None
    }
  }

  /**
    * Method for parsing the CrateVersion fields in a CrateRelease object
    * @param jsObject object to be parsed
    * @return
    */
  def parseCrateVersionsJsonToCrateVersionsObject(
      jsObject: JsObject): Option[CrateVersion] = {
    try {
      val id: Int = this.getIntFieldFromCrate(jsObject, "id").get
      val crate: String = this.getStringFieldFromCrate(jsObject, "crate").get
      val num: String = this.getStringFieldFromCrate(jsObject, "num").get
      val dl_path: String =
        this.getStringFieldFromCrate(jsObject, "dl_path").get
      val readme_path: String =
        this.getStringFieldFromCrate(jsObject, "readme_path").get
      val updated_at: Timestamp =
        this.getDateFieldFromCrate(jsObject, "updated_at").get
      val created_at: Timestamp =
        this.getDateFieldFromCrate(jsObject, "created_at").get
      val downloads: Int = this.getIntFieldFromCrate(jsObject, "downloads").get
      val features
        : CrateVersionFeatures = new CrateVersionFeatures // this is always empty
      val yanked: Boolean = this.getBoolFieldFromCrate(jsObject, "yanked").get
      val license: String =
        this.getStringFieldFromCrate(jsObject, "license").get
      val links: CrateVersionLinks =
        this
          .parseCrateVersionLinksJsonToCrateVersionLinksObject(
            jsObject.fields("links").asJsObject())
          .get
      val crate_size: Option[Int] =
        this.getIntFieldFromCrate(jsObject, "crate_size")
      val published_by: Option[CrateVersionPublishedBy] = try {
        this.parseCrateVersionPublishedByJsonToCrateVersionPublishedByObject(
          jsObject.fields("published_by").asJsObject())
      } catch {
        case _: Throwable => None
      }

      Some(
        CrateVersion(id,
                     crate,
                     num,
                     dl_path,
                     readme_path,
                     updated_at,
                     created_at,
                     downloads,
                     features,
                     yanked,
                     license,
                     links,
                     crate_size,
                     published_by)
      )
    } catch {
      case _: Throwable =>
        printf("\nparsing from Json to CrateVersion object failed")
        None
    }
  }

  /**
    * Method for parsing the Links field in a Crate object
    * @param jsObject object to be parsed
    * @return
    */
  def parseCrateLinksJsonToCrateLinksObject(
      jsObject: JsObject): Option[CrateLinks] = {
    try {
      val version_downloads: String =
        this.getStringFieldFromCrate(jsObject, "version_downloads").get
      val versions: Option[String] =
        this.getStringFieldFromCrate(jsObject, "versions")
      val owners: String = this.getStringFieldFromCrate(jsObject, "owners").get
      val owner_team: String =
        this.getStringFieldFromCrate(jsObject, "owner_team").get
      val owner_user: String =
        this.getStringFieldFromCrate(jsObject, "owner_user").get
      val reverse_dependencies: String =
        this.getStringFieldFromCrate(jsObject, "reverse_dependencies").get

      Some(
        CrateLinks(version_downloads,
                   versions,
                   owners,
                   owner_team,
                   owner_user,
                   reverse_dependencies)
      )
    } catch {
      case _: Throwable =>
        printf("\nparsing crate links failed.")
        None
    }
  }

  /**
    * Method for parsing the CrateVersionLinks field in a CrateVersion object
    * @param jsObject object to be parsed
    * @return
    */
  def parseCrateVersionLinksJsonToCrateVersionLinksObject(
      jsObject: JsObject): Option[CrateVersionLinks] = {
    try {
      val dependencies: String =
        this.getStringFieldFromCrate(jsObject, "dependencies").get
      val version_downloads: String =
        this.getStringFieldFromCrate(jsObject, "version_downloads").get
      val authors: String =
        this.getStringFieldFromCrate(jsObject, "authors").get

      Some(CrateVersionLinks(dependencies, version_downloads, authors))
    } catch {
      case _: Throwable =>
        printf("\nparsing crate version links returned a None somehow.")
        None
    }
  }

  /**
    * Method for parsing the CrateVersionPublishedBy field in a CrateVersion object
    * @param jsObject object to be parsed
    * @return
    */
  def parseCrateVersionPublishedByJsonToCrateVersionPublishedByObject(
      jsObject: JsObject): Option[CrateVersionPublishedBy] = {
    try {
      val id: Int = this.getIntFieldFromCrate(jsObject, "id").get
      val login: String = this.getStringFieldFromCrate(jsObject, "login").get
      val name: Option[String] = this.getStringFieldFromCrate(jsObject, "name")
      val avatar: String = this.getStringFieldFromCrate(jsObject, "avatar").get
      val url: String = this.getStringFieldFromCrate(jsObject, "url").get

      Some(CrateVersionPublishedBy(id, login, name, avatar, url))
    } catch {
      case _: Throwable =>
        printf("\nparsing crate version links returned a None somehow.")
        None
    }
  }

  /**
    * Method for parsing the CrateKeyword field in a Crate object
    * @param jsObject object to be parsed
    * @return
    */
  def parseCrateKeywordsJsonToCrateKeywordsObject(
      jsObject: JsObject): Option[CrateKeyword] = {
    try {
      val id: String = this.getStringFieldFromCrate(jsObject, "id").get
      val keyword: String =
        this.getStringFieldFromCrate(jsObject, "keyword").get
      val created_at: String =
        this.getStringFieldFromCrate(jsObject, "created_at").get
      val crates_cnt = this.getIntFieldFromCrate(jsObject, "crates_cnt").get

      Some(CrateKeyword(id, keyword, created_at, crates_cnt))
    } catch {
      case _: Throwable =>
        printf("\nparsing the crate keyword returned a None somehow.")
        None
    }
  }

  /**
    * Method for parsing the CrateCategory field in a Crate object
    * @param jsObject object to be parsed
    * @return
    */
  def parseCrateCategoryJsonToCrateCategoryObject(
      jsObject: JsObject): Option[CrateCategory] = {
    try {
      val id: String = this.getStringFieldFromCrate(jsObject, "id").get
      val category: String =
        this.getStringFieldFromCrate(jsObject, "category").get
      val slug: String = this.getStringFieldFromCrate(jsObject, "slug").get
      val description: String =
        this.getStringFieldFromCrate(jsObject, "description").get
      val created_at: String =
        this.getStringFieldFromCrate(jsObject, "created_at").get
      val crates_cnt: Int =
        this.getIntFieldFromCrate(jsObject, "crates_cnt").get

      Some(
        CrateCategory(id, category, slug, description, created_at, crates_cnt))
    } catch {
      case _: Throwable =>
        printf("\nparsing the crate category returned a None somehow.")
        None
    }
  }

  /** Below methods are used to parse single fields in Json to their respective types */
  def getStringFieldFromCrate(jsObject: JsObject,
                              field: String): Option[String] = {
    try {
      Some(jsObject.fields(field).asInstanceOf[JsString].value)
    } catch {
      case _: Throwable =>
        None
    }
  }

  def getIntFieldFromCrate(jsObject: JsObject, field: String): Option[Int] = {
    try {
      Some(jsObject.fields(field).asInstanceOf[JsNumber].value.toInt)
    } catch {
      case _: Throwable =>
        None
    }
  }

  def getBoolFieldFromCrate(jsObject: JsObject,
                            field: String): Option[Boolean] = {
    try {
      Some(jsObject.fields(field).asInstanceOf[JsBoolean].value)
    } catch {
      case _: Throwable =>
        None
    }
  }

  def getDateFieldFromCrate(jsObject: JsObject,
                            field: String): Option[Timestamp] = {
    try {
      val fieldValue: String =
        jsObject.fields(field).asInstanceOf[JsString].value
      val dateField: Date = dateFormat.parse(fieldValue)
      val cal = Calendar.getInstance

      cal.setTime(dateField)
      val time = new Timestamp(cal.getTimeInMillis)

      Some(time)
    } catch {
      case _: Throwable =>
        println("Failed parsing the Date")
        None
    }
  }

  def getListOfIntsFieldFromCrate(jsObject: JsObject,
                                  field: String): Option[List[Int]] = {
    try {
      Some(
        jsObject
          .fields(field)
          .asInstanceOf[JsArray]
          .elements
          .toList
          .map(x => x.asInstanceOf[JsNumber])
          .map(x => x.value)
          .map(x => x.toInt))
    } catch {
      case _: Throwable =>
        None
    }
  }

  def getListOfStringsFieldFromCrate(jsObject: JsObject,
                                     field: String): Option[List[String]] = {
    try {
      Some(
        jsObject
          .fields(field)
          .asInstanceOf[JsArray]
          .elements
          .toList
          .map(x => x.asInstanceOf[JsString])
          .map(x => x.value))
    } catch {
      case _: Throwable =>
        None
    }
  }
}
