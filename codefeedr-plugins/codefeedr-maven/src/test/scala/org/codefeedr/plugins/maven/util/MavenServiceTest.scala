package org.codefeedr.plugins.maven.util

import org.scalatest.FunSuite

import scala.xml.Node

class MavenServiceTest extends FunSuite{

  //Dit kan evt in een before, moet nog ff uitzoeken hoe
  val dependencyNodesMinimal: Node = xml.XML.loadString("<dependencies><dependency><groupId>org.drools</groupId><artifactId>drools-core</artifactId><version>4.0.7</version></dependency></dependencies>")
  val dependencyNodesFull: Node = xml.XML.loadString("<dependencies><dependency><groupId>org.test</groupId><artifactId>test-core</artifactId><version>4.2.0</version><type>jar</type><scope>test</scope><optional>true</optional></dependency></dependencies>")
  val dependencyNodesMultiple: Node = xml.XML.loadString("<dependencies><dependency><groupId>org.eurekaclinical</groupId><artifactId>javautil</artifactId></dependency><dependency><groupId>org.drools</groupId><artifactId>drools-core</artifactId></dependency></dependencies>")
  val dependencyNodesProjectVersion: Node = xml.XML.loadString("<dependencies><dependency><groupId>org.drools</groupId><artifactId>drools-core</artifactId><version>${project.version}</version></dependency></dependencies>")

  val licenseNodesNoComment: Node = xml.XML.loadString("<licenses><license><name>The Apache Software License, Version 2.0</name><url>http://www.apache.org/licenses/LICENSE-2.0.txt</url><distribution>repo</distribution></license></licenses>")
  val licenseNodesWithComment: Node = xml.XML.loadString("<licenses><license><name>MIT</name><url>https://opensource.org/licenses/MIT</url><distribution>repo</distribution><comments>Example comment</comments></license></licenses>")

  val repoNodesGood: Node = xml.XML.loadString("<repositories><repository><id>codehausSnapshots</id><name>Codehaus Snapshots</name><url>http://snapshots.maven.codehaus.org/maven2</url></repository></repositories>")
  val scmNode: Node = xml.XML.loadString("<scm><connection>127.0.0.1/</connection><developerConnection>127.0.0.1/svn/</developerConnection><tag>HEAD</tag><url>http://127.0.0.1</url></scm>")
  val issueNode: Node = xml.XML.loadString("<issueManagement><system>Bugzilla</system><url>http://127.0.0.1/bugzilla/</url></issueManagement>")
  val organizationNode: Node = xml.XML.loadString("<organization><name>Codehaus Mojo</name><url>http://mojo.codehaus.org</url></organization>")
  val parentNode: Node = xml.XML.loadString("<parent><groupId>org.codehaus.mojo</groupId><artifactId>my-parent</artifactId><version>2.0</version><relativePath>../my-parent</relativePath></parent>")
  val emptyNode: Seq[Node] = dependencyNodesMinimal.child.filter(item => item.label == "non-existing")

  val projectNoDepNoLicense: Node = xml.XML.loadString("<project><modelVersion>4.0.0</modelVersion><groupId>exampleGroupId</groupId><artifactId>exampleArtifactId</artifactId><version>6.9.420</version></project>")
  val projectNoDepNoLicenseWithParent: Node = xml.XML.loadString("<project><modelVersion>4.0.0</modelVersion><parent><groupId>exampleGroupId</groupId><artifactId>exampleArtifactId</artifactId><version>6.9.420</version></parent></project>")
  val projectDepsAndLic: Node = xml.XML.loadString("<project><modelVersion>4.0.0</modelVersion><groupId>exampleGroupId</groupId><artifactId>exampleArtifactId</artifactId><version>6.9.420</version>" +
    "<dependencies><dependency><groupId>org.drools</groupId><artifactId>drools-core</artifactId><version>4.0.7</version></dependency></dependencies>" +
    "<licenses><license><name>MIT</name><url>https://opensource.org/licenses/MIT</url><distribution>repo</distribution><comments>Example comment</comments></license></licenses>" +
    "</project>")

  //----------------- parseDependencies -----------------//
  test("parseDepsNoVersionTest"){
    //Parse node
    val dependencyNodesNoVersion = xml.XML.loadString("<dependencies><dependency><groupId>io.cucumber</groupId><artifactId>cucumber-core</artifactId></dependency></dependencies>")
    val res = MavenService.parseDependencies(dependencyNodesNoVersion, "").get

    //Assert the result
    assert(res.size == 1)
    assert(res.head.groupId == "io.cucumber")
    assert(res.head.artifactId == "cucumber-core")
    assert(res.head.version.isEmpty)
  }

  test("parseDepsWithVersionTest"){
    val res = MavenService.parseDependencies(dependencyNodesMinimal, "").get

    assert(res.size == 1)
    assert(res.head.groupId == "org.drools")
    assert(res.head.artifactId == "drools-core")
    assert(res.head.version.get == "4.0.7")
  }

  test("parseDepsOptionalFieldsEmpty"){
    val res = MavenService.parseDependencies(dependencyNodesMinimal, "").get

    assert(res.size == 1)
    assert(res.head.`type`.isEmpty)
    assert(res.head.scope.isEmpty)
    assert(res.head.optional.isEmpty)
  }

  test("parseDepsOptionalFieldsFull"){
    val res = MavenService.parseDependencies(dependencyNodesFull, "").get

    assert(res.size == 1)
    assert(res.head.`type`.get == "jar")
    assert(res.head.scope.get == "test")
    assert(res.head.optional.get == true)
  }

  test("parseDepsMultipleTest"){
    val res = MavenService.parseDependencies(dependencyNodesMultiple, "").get

    assert(res.size == 2)
    assert(res.head.groupId == "org.eurekaclinical")
    assert(res(1).groupId == "org.drools")
  }


  test("parseDepsNoChildTest"){
    val res = MavenService.parseDependencies(emptyNode, "")

    assert(res.isEmpty)
  }

  test("parseDepProjectVersionTest"){
    val projectVersion = "1.2.3"
    val res = MavenService.parseDependencies(dependencyNodesProjectVersion, projectVersion)

    assert(res.get.head.version.get == projectVersion)
  }


  //----------------- parseLicenses -----------------//
  test("parseLicensesTest"){
    val res = MavenService.parseLicenses(licenseNodesNoComment).get

    assert(res.size == 1)
    assert(res.head.url == "http://www.apache.org/licenses/LICENSE-2.0.txt")
    assert(res.head.name == "The Apache Software License, Version 2.0")
    assert(res.head.distribution == "repo")
    assert(res.head.comments.isEmpty)
  }

  test("parseLicenceswithCommentsTest"){
    val res = MavenService.parseLicenses(licenseNodesWithComment).get

    assert(res.size == 1)
    assert(res.head.url == "https://opensource.org/licenses/MIT")
    assert(res.head.name == "MIT")
    assert(res.head.distribution == "repo")
    assert(res.head.comments.get == "Example comment")
  }

  test("parseLicenseEmpty"){
    val res = MavenService.parseLicenses(emptyNode)

    assert(res.isEmpty)
  }

  //------------------ parseRepository ------------------//
  test("parseRepositoryGoodTest"){
    val res = MavenService.parseRepositories(repoNodesGood).get

    assert(res.size == 1)
    assert(res.head.id == "codehausSnapshots")
    assert(res.head.name == "Codehaus Snapshots")
    assert(res.head.url == "http://snapshots.maven.codehaus.org/maven2")
  }

  test("parseRepositoryBadTest"){
    val res = MavenService.parseRepositories(emptyNode)

    assert(res.isEmpty)
  }

  //-----------------------parseSCM-----------------------//
  test("parseSCMTest"){
    val res = MavenService.parseSCM(scmNode).get

    assert(res.connection == "127.0.0.1/")
    assert(res.developerConnection.get.contains("127.0.0.1/svn/"))
    assert(res.tag.get == "HEAD")
    assert(res.url == "http://127.0.0.1")
  }

  test("parseSCMEmptyTest"){
    val res = MavenService.parseSCM(emptyNode)

    assert(res.isEmpty)
  }

  //-------------------parseIssueManagement----------------//
  test("parseIssueManagementTest"){
    val res = MavenService.parseIssueManagement(issueNode).get

    assert(res.system == "Bugzilla")
    assert(res.url == "http://127.0.0.1/bugzilla/")
  }

  test("parseIssueManagementEmptyTest"){
    val res = MavenService.parseIssueManagement(emptyNode)

    assert(res.isEmpty)
  }

  //-----------------parseOrganization---------------------//
  test("parseOrganizationTest"){
    val res = MavenService.parseOrganization(organizationNode).get

    assert(res.name == "Codehaus Mojo")
    assert(res.url == "http://mojo.codehaus.org")
  }

  test("parseOrganizationEmptyTest"){
    val res = MavenService.parseOrganization(emptyNode)

    assert(res.isEmpty)
  }

  //----------------parseParent--------------------------//
  test("parseParentTest"){
    val res = MavenService.parseParent(parentNode).get

    assert(res.groupId == "org.codehaus.mojo")
    assert(res.artifactId == "my-parent")
    assert(res.version == "2.0")
    assert(res.relativePath.get == "../my-parent")
  }

  test("parseParentEmptyTest"){
    val res = MavenService.parseParent(emptyNode)

    assert(res.isEmpty)
  }

  //----------------- xmlToMavenProject -----------------//
  test("xmlToMavenProjectNoDepNoLicNoParentTest"){
    val res = MavenService.xmlToMavenProject(projectNoDepNoLicense).get

    assert(res.groupId == "exampleGroupId")
    assert(res.artifactId == "exampleArtifactId")
    assert(res.modelVersion == "4.0.0")
    assert(res.version == "6.9.420")
    assert(res.parent.isEmpty)
  }

  test("xmlToMavenProjectNoDepNoLicWithParentTest"){
    val res = MavenService.xmlToMavenProject(projectNoDepNoLicenseWithParent).get

    //assert the fields are inherited correctly
    assert(res.modelVersion == "4.0.0")
    assert(res.groupId == "exampleGroupId")
    assert(res.artifactId == "exampleArtifactId")
    assert(res.version == "6.9.420")
    //assert the parent obj is correct
    assert(res.parent.get.groupId == "exampleGroupId")
    assert(res.parent.get.artifactId == "exampleArtifactId")
    assert(res.parent.get.version == "6.9.420")
    assert(res.parent.get.relativePath.isEmpty)
  }

  test("xmlToMavenProjectDepsAndLicTest"){
    val res = MavenService.xmlToMavenProject(projectDepsAndLic).get

    assert(res.groupId == "exampleGroupId")
    assert(res.dependencies.get.head.groupId == "org.drools")
    assert(res.licenses.get.head.name == "MIT")
  }

  //----------------- getProject -----------------//
  test("getProjectNoProject"){
    val res= MavenService.getProject("thisDoesntExist")

    assert(res.isEmpty)
  }

}
