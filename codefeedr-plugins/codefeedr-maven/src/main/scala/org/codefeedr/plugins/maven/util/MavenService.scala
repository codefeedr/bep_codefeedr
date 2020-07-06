package org.codefeedr.plugins.maven.util

import org.apache.logging.log4j.scala.Logging
import org.codefeedr.stages.utilities.HttpRequester
import org.codefeedr.plugins.maven.protocol.Protocol._
import scalaj.http.Http

import scala.util.control.Breaks._
import scala.xml.{Node, XML}

/** Services to retrieve a project from the Maven APi. */
object MavenService extends Logging with Serializable {

  /** Retrieve the API url. */
  private val url = "https://repo1.maven.org/maven2/"

  /** Retrieves a Maven project.
    *
    * @param projectName the name of the project.
    * @return an optional MavenProject.
    */
  def getProject(projectName: String): Option[MavenProject] = {

    /** Retrieve the project. */
    val rawProject = getProjectRaw(projectName)
    if (rawProject.isEmpty) {
      logger.error(s"Couldn't retrieve Maven project with name $projectName.")

      return None
    }

    var xml: scala.xml.Node = null
    try {
      xml = XML.loadString(rawProject.get)
    } catch {
      case _: Exception =>
        logger.error(s"Couldn't convert string to xml with name $projectName.")
        return None
    }

    /** Extract into an optional if it can't be parsed. */
    val project = xmlToMavenProject(xml)

    if (project.isEmpty) {
      logger.error(
        s"Couldn't retrieve Maven project with name $projectName and xml $xml.")

      return None
    }

    /** Forward the project */
    project
  }

  /**
    * Turns xml of a pom.xml to a MavenProject
    *
    * @param node the xml containing the pom.xml
    * @return an option of a MavenProject Some(project) if parsed successfully, else otherwise
    */
  def xmlToMavenProject(node: scala.xml.Node): Option[MavenProject] = {
    // Set initial values
    // Modelversion is always the same, so read the value
    val modelVersion = (node \ "modelVersion").text

    // Sometimes these are inherited from parent
    var groupId = ""
    var artifactId = ""
    var version = ""

    // Get the parent of a project
    val parent = parseParent(getInnerNode(node, "parent"))

    // Sometimes groupId, artifactId and version are inside parent, sometimes not
    groupId = (node \ "groupId").text
    artifactId = (node \ "artifactId").text
    version = (node \ "version").text

    // If (one of) the fields is/are empty, inherit it/them from the parent
    if (groupId == "") {
      groupId = parent.get.groupId
    }
    if (artifactId == "") {
      artifactId = parent.get.artifactId
    }
    if (version == "") {
      version = parent.get.version
    }

    // Get dependencies
    val dependencies =
      parseDependencies(getInnerNode(node, "dependencies"), version)
    // Get licenses
    val licenses = parseLicenses(getInnerNode(node, "licenses"))
    // Get repositories (often empty)
    val repositories = parseRepositories(getInnerNode(node, "repositories"))
    // Get organization
    val organization = parseOrganization(getInnerNode(node, "organization"))

    // Parse the packaging information
    val packaging: Option[String] = checkEmpty((node \ "packaging").text)

    // Get IssueManagement
    val issueManagement = parseIssueManagement(
      getInnerNode(node, "issueManagement"))
    // Get SCM
    val scm = parseSCM(getInnerNode(node, "scm"))

    Some(
      MavenProject(modelVersion,
                   groupId,
                   artifactId,
                   version,
                   parent,
                   dependencies,
                   licenses,
                   repositories,
                   organization,
                   packaging,
                   issueManagement,
                   scm))
  }

  /**
    * This function is used as often a check is needed to determine whether a option field was filled or not
    *
    * @param in the string which needs to be checked if it is empty
    * @return Option of $in if filled, None if $in == ""
    */
  def checkEmpty(in: String): Option[String] = {
    if (in == "") None else Some(in)
  }

  /**
    * Helper function for xmlToMavenProject as often we need to get an inner node with a certain label
    *
    * @param node  the node from which to get the inner node
    * @param label the label for the inner node
    * @return a sequence of inner nodes corresponding to $label
    */
  def getInnerNode(node: Node, label: String): Seq[Node] = {
    node.child.filter(item => item.label == label)
  }

  def parseSCM(nodes: Seq[Node]): Option[SCM] = {
    if (nodes.isEmpty) {
      None
    } else {
      val scm = nodes.head
      val connection = (scm \ "connection").text
      val devConnection = checkEmpty((scm \ "developerConnection").text)
      val tag = checkEmpty((scm \ "tag").text)
      val url = (scm \ "url").text
      Some(SCM(connection, devConnection, tag, url))
    }
  }

  /**
    * Parses the issueManagement from a xml node
    *
    * @param nodes the node which (possibly) contains the organization information
    * @return Option of IssueManagement, Some(IM) if $nodes was nonEmpty, None otherwise
    */
  def parseIssueManagement(nodes: Seq[Node]): Option[IssueManagement] = {
    if (nodes.isEmpty) {
      None
    } else {
      val issueManagement = nodes.head
      val system = (issueManagement \ "system").text
      val url = (issueManagement \ "url").text
      Some(IssueManagement(system, url))
    }
  }

  /**
    * Parses the organizatino from a xml node
    *
    * @param nodes the node which (possibly) contains the organization information
    * @return Option of Organization, Some(org) if $nodes was nonEmpty, None otherwise
    */
  def parseOrganization(nodes: Seq[Node]): Option[Organization] = {
    if (nodes.isEmpty) {
      None
    } else {
      val org = nodes.head
      val name = (org \ "name").text
      val url = (org \ "url").text
      Some(Organization(name, url))
    }
  }

  /**
    * Parses the parent from a xml node
    *
    * @param parentNode the node which (possibly) contains the parent information
    * @return Option of Parent, Some(parent) if $parentNode was nonEmpty, None otherwise
    */
  def parseParent(parentNode: Seq[Node]): Option[Parent] = {
    // If the parentNode is empty, return None
    if (parentNode.isEmpty) {
      None
    }
    // Else parse and return the information of the Parent xml
    else {
      val parent = parentNode.head
      val groupId = (parent \ "groupId").text
      val artifactId = (parent \ "artifactId").text
      val version = (parent \ "version").text
      val relativePath: Option[String] = checkEmpty(
        (parent \ "relativePath").text)
      Some(Parent(groupId, artifactId, version, relativePath))
    }
  }

  /**
    * Parses the repositories from a xml node
    *
    * @param nodes the nodes which (possibly) contain the repository information
    * @return Option of List[Repository], Some(list) if $nodes was filled, None otherwise
    */
  def parseRepositories(nodes: Seq[Node]): Option[List[Repository]] = {
    //If there are repositories parse them
    if (nodes.nonEmpty) {
      var repoList = List[Repository]()
      for (n <- nodes.head.child) {
        breakable {
          //The first node in the xml structure is always empty so we skip this one as it contains no info
          if (n.toString().startsWith("\n")) {
            break()
          }
          val currentId = (n \ "id").text
          val currentName = (n \ "name").text
          val currentUrl = (n \ "url").text
          repoList = Repository(currentId, currentName, currentUrl) :: repoList
        }
      }
      return Some(repoList.reverse)
    }
    //If there are no repositories, return None
    None
  }

  /**
    * Parses the licenses from a xml node
    *
    * @param nodes the nodes which (possibly) contain the licenses information
    * @return Option of List[License], Some(list) if $nodes was filled, None otherwise
    */
  def parseLicenses(nodes: Seq[Node]): Option[List[License]] = {
    //If there are licenses parse them
    if (nodes.nonEmpty) {
      var licensesList = List[License]()
      for (n <- nodes.head.child) {
        breakable {
          //The first node in the xml structure is always empty so we skip this one as it contains no info
          if (n.toString().startsWith("\n")) {
            break()
          }
          val currentName = (n \ "name").text
          val currentUrl = (n \ "url").text
          val currentDistribution = (n \ "distribution").text
          val currentComments: Option[String] =
            checkEmpty((n \ "comments").text)

          licensesList = License(currentName,
                                 currentUrl,
                                 currentDistribution,
                                 currentComments) :: licensesList
        }
      }
      return Some(licensesList.reverse)
    }
    //If there are no licenses, return None
    None
  }

  /**
    * Returns a list of Dependencies if the xml node contains them
    *
    * @param node the list of nodes from which the depencies will be extracted
    * @return An option of a list of dependencies, Some(list) if $node contained them, None otherwise
    */
  def parseDependencies(node: Seq[Node],
                        projectVersion: String): Option[List[Dependency]] = {
    //If there are dependencies parse them
    if (node.nonEmpty) {
      var dependencyList = List[Dependency]()
      for (n <- node.head.child) {
        breakable {
          //The first node in the xml structure is always empty so we skip this one as it contains no info
          if (n.toString().startsWith("\n")) {
            break()
          }
          val currentGroupId = (n \ "groupId").text
          val currentArtifactId = (n \ "artifactId").text
          var currentVersion: Option[String] = checkEmpty((n \ "version").text)
          if (currentVersion.getOrElse(None) == "${project.version}") {
            currentVersion = Some(projectVersion)
          }
          val dType: Option[String] = checkEmpty((n \ "type").text)
          val scope: Option[String] = checkEmpty((n \ "scope").text)

          val optionalString: String = (n \ "optional").text
          var optional: Option[Boolean] = None
          if (optionalString == "true" || optionalString == "false") {
            optional = Some(optionalString.toBoolean)
          }

          dependencyList = Dependency(currentGroupId,
                                      currentArtifactId,
                                      currentVersion,
                                      dType,
                                      scope,
                                      optional) :: dependencyList
        }
      }
      // As we prepended everything, reverse the list to obtain the original order
      return Some(dependencyList.reverse)
    }
    // If there are no dependencies, return None
    None
  }

  /** Returns a project as a raw string.
    *
    * @param endpoint the end_point to do the request.
    * @return an optional String.
    */
  def getProjectRaw(endpoint: String): Option[String] = {
    val response = try {
      val request = Http(url + endpoint).headers(getHeaders)
      new HttpRequester().retrieveResponse(request)
    } catch {
      case _: Throwable => return None
    }

    Some(response.body)
  }

  /** Add a user-agent with contact details. */
  def getHeaders: List[(String, String)] =
    ("User-Agent", "CodeFeedr-Maven/1.0 Contact: zonneveld.noordwijk@gmail.com") :: Nil

}
