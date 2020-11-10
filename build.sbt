import sbt.Credentials
import sbt.Keys.{credentials, name, publishMavenStyle}

lazy val scala212 = "2.12.8"
lazy val supportedScalaVersions = List(scala212)

ThisBuild / organization := "org.codefeedr"
ThisBuild / organizationName := "CodeFeedr"
ThisBuild / organizationHomepage := Some(url("http://codefeedr.org"))

ThisBuild / githubOwner := "codefeedr"
ThisBuild / githubRepository := "codefeedr"
ThisBuild / githubTokenSource := TokenSource.GitConfig("github.token") || TokenSource.Environment("GITHUB_TOKEN")

ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/codefeedr/codefeedr"),
    "scm:git@github.com:codefeedr/codefeedr.git"
  )
)

ThisBuild / developers := List(
  Developer(
    id    = "wzorgdrager",
    name  = "Wouter Zorgdrager",
    email = "W.D.Zorgdrager@tudelft.nl",
    url   = url("http://www.github.com/wzorgdrager")
  )
)

ThisBuild / description := "CodeFeedr provides an infrastructure on top of Apache Flink for more advanced stream architectures."
ThisBuild / licenses := List("Apache 2" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt"))
ThisBuild / homepage := Some(url("https://github.com/codefeedr/codefeedr"))

ThisBuild / version := "1.0.10"
ThisBuild / organization := "org.codefeedr"
ThisBuild / scalaVersion := scala212


parallelExecution in Test := false

/// PROJECTS

val projectPrefix = "codefeedr-"
val pluginPrefix = projectPrefix + "plugin-"

lazy val root = (project in file("."))
  .settings(settings ++ noPublishSettings)
  .aggregate(core,
    pluginMongodb,
    pluginElasticSearch,
    pluginGitHub,
    pluginRabbitMQ,
    pluginGHTorrent,
    pluginPypi)

lazy val core = (project in file("codefeedr-core"))
  .settings(
    name := projectPrefix + "core",
    settings,
    assemblySettings,
    unmanagedBase := baseDirectory.value / "../lib",
    libraryDependencies ++= commonDependencies ++ Seq(
      // JSONBuffer
      dependencies.json4s,
      dependencies.jackson,
      dependencies.json4sExt,

      // Http
      dependencies.httpj,

      // KafkaBuffer
      dependencies.kafkaClient,
      dependencies.flinkKafka,

      // RedisKeyManager
      dependencies.redis,

      // Schema exposure
      dependencies.zookeeper,

      // BSON serialization
      dependencies.mongo,

      // Kryo serialization
      dependencies.kryoChill,

      // Avro schema exposure
      dependencies.avro,

      // Embedded redis to test key management
      dependencies.embeddedRedis,

      // Embedded kafka for integration tests
      dependencies.embeddedKafka
    )
  )

lazy val pluginMongodb = (project in file("codefeedr-plugins/codefeedr-mongodb"))
  .settings(
    name := pluginPrefix + "mongodb",
    settings,
    assemblySettings,
    libraryDependencies ++= commonDependencies ++ Seq(
      dependencies.mongo,
      dependencies.embeddedMongo
    )
  )
  .dependsOn(core)

lazy val pluginElasticSearch = (project in file("codefeedr-plugins/codefeedr-elasticsearch"))
  .settings(
    name := pluginPrefix + "elasticsearch",
    settings,
    assemblySettings,
    libraryDependencies ++= commonDependencies ++ Seq(
      dependencies.flinkElasticSearch
    )
  )
  .dependsOn(core)

lazy val pluginGitHub = (project in file("codefeedr-plugins/codefeedr-github"))
  .settings(
    name := pluginPrefix + "github",
    description := "GitHub plugin",
    settings,
    assemblySettings,
    libraryDependencies ++= commonDependencies ++ Seq(
      dependencies.httpj,
      dependencies.json4s,
      dependencies.jackson,
      dependencies.json4sExt
    )
  )
  .dependsOn(core)

lazy val pluginRabbitMQ = (project in file("codefeedr-plugins/codefeedr-rabbitmq"))
  .settings(
    name := pluginPrefix + "rabbitmq",
    settings,
    assemblySettings,
    libraryDependencies ++= commonDependencies ++ Seq(
      dependencies.flinkRabbitMQ,
      //dependencies.embeddedRabbitMQ
    )
  )
  .dependsOn(core)

lazy val pluginGHTorrent = (project in file("codefeedr-plugins/codefeedr-ghtorrent"))
  .settings(
    name := pluginPrefix + "ghtorrent",
    settings,
    assemblySettings,
    libraryDependencies ++= commonDependencies ++ Seq(
      dependencies.flinkRabbitMQ,
      //dependencies.embeddedRabbitMQ
    )
  ).dependsOn(core)

lazy val pluginPypi = (project in file("codefeedr-plugins/codefeedr-pypi"))
  .settings(
    name := pluginPrefix + "pypi",
    settings,
    assemblySettings,
    libraryDependencies ++= commonDependencies ++ Seq(
    )
  ).dependsOn(core)

lazy val dependencies =
  new {
    val flinkVersion       = "1.9.1"
    val json4sVersion      = "3.6.4"
    val log4jVersion       = "2.11.0"
    val log4jScalaVersion  = "11.0"


    val loggingApi         = "org.apache.logging.log4j"   % "log4j-api"                      % log4jVersion
    val loggingCore        = "org.apache.logging.log4j"   % "log4j-core"                     % log4jVersion      % Runtime
    val loggingScala       = "org.apache.logging.log4j"  %% "log4j-api-scala"                % log4jScalaVersion

    val flink              = "org.apache.flink"          %% "flink-scala"                    % flinkVersion      % Provided
    val flinkStreaming     = "org.apache.flink"          %% "flink-streaming-scala"          % flinkVersion      % Provided
    val flinkKafka         = "org.apache.flink"          %% "flink-connector-kafka"          % flinkVersion
    val flinkRuntimeWeb    = "org.apache.flink"          %% "flink-runtime-web"              % flinkVersion      % Provided
    val flinkElasticSearch = "org.apache.flink"          %% "flink-connector-elasticsearch6" % flinkVersion
    val flinkRabbitMQ      = "org.apache.flink"          %% "flink-connector-rabbitmq"       % flinkVersion

    val redis              = "net.debasishg"             %% "redisclient"                    % "3.6"
    val kafkaClient        = "org.apache.kafka"           % "kafka-clients"                  % "2.4.0"
    val zookeeper          = "org.apache.zookeeper"       % "zookeeper"                      % "3.4.9"

    val json4s             = "org.json4s"                %% "json4s-scalap"                  % json4sVersion
    val jackson            = "org.json4s"                %% "json4s-jackson"                 % json4sVersion
    val json4sExt          = "org.json4s"                %% "json4s-ext"                     % json4sVersion

    val mongo              = "org.mongodb.scala"         %% "mongo-scala-driver"             % "2.3.0"

    val httpj              = "org.scalaj"                %% "scalaj-http"                    % "2.4.0"

    val kryoChill          = "com.twitter"               %% "chill"                          % "0.9.1"

    val scalactic          = "org.scalactic"             %% "scalactic"                      % "3.2.2"           % Test
    val scalatest          = "org.scalatest"             %% "scalatest"                      % "3.2.2"           % Test
    val scalamock          = "org.scalamock"             %% "scalamock"                      % "4.1.0"           % Test
    val mockito            = "org.mockito"                % "mockito-all"                    % "1.10.19"         % Test
    val embeddedRedis      = "com.github.sebruck"        %% "scalatest-embedded-redis"       % "0.3.0"           % Test

    val embeddedKafka      = "io.github.embeddedkafka"   %% "embedded-kafka"                 % "2.6.0"           % Test
    val embeddedMongo      = "com.github.simplyscala"    %% "scalatest-embedmongo"           % "0.2.4"           % Test
    //val embeddedRabbitMQ   = "io.arivera.oss"            %% "embedded-rabbitmq"              % "1.3.0"           % Test

    val avro               = "org.apache.avro"            % "avro"                           % "1.8.2"
  }

lazy val commonDependencies = Seq(
  dependencies.flink,
  dependencies.flinkStreaming,

  dependencies.loggingApi,
  dependencies.loggingCore,
  dependencies.loggingScala,

  dependencies.scalactic,
  dependencies.scalatest,
  dependencies.scalamock,
  dependencies.mockito
)

// SETTINGS

lazy val settings = commonSettings

lazy val commonSettings = Seq(
  test in assembly := {},
  scalacOptions ++= compilerOptions,
  resolvers ++= Seq(
    "confluent"                               at "http://packages.confluent.io/maven/",
    "Apache Development Snapshot Repository"  at "https://repository.apache.org/content/repositories/snapshots/",
    "Artima Maven Repository"                 at "http://repo.artima.com/releases",
    Resolver.mavenLocal,
    Resolver.jcenterRepo
  ),
  publishMavenStyle in ThisBuild := true,
  publishTo in ThisBuild := Some(
    if (version.value.trim.endsWith("SNAPSHOT"))
      Opts.resolver.sonatypeSnapshots
    else
      Opts.resolver.sonatypeStaging
  ),
  ThisBuild / pomIncludeRepository := { _ => false },
  crossScalaVersions := supportedScalaVersions
)

lazy val noPublishSettings = Seq(
  publish := {},
  publishLocal := {},
  publishArtifact := false,
  // sbt-pgp's publishSigned task needs this defined even though it is not publishing.
  publishTo := Some(Resolver.file("Unused transient repository", file("target/unusedrepo"))),
  crossScalaVersions := Nil
)

lazy val compilerOptions = Seq(
  //  "-unchecked",
  //  "-feature",
  //  "-language:existentials",
  //  "-language:higherKinds",
  //  "-language:implicitConversions",
  //  "-language:postfixOps",
  //  "-deprecation",
  "-encoding",
  "utf8"
)

lazy val assemblySettings = Seq(
  assemblyJarName in assembly := name.value + ".jar",
  test in assembly := {},
  assemblyMergeStrategy in assembly := {
    case PathList("META-INF", xs @ _*)  => MergeStrategy.discard
    case "log4j.properties"             => MergeStrategy.first
    case x =>
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      oldStrategy(x)
  }
)

// MAKING FLINK WORK

// make run command include the provided dependencies
Compile / run  := Defaults.runTask(Compile / fullClasspath,
                                   Compile / run / mainClass,
                                   Compile / run / runner
                                  ).evaluated

// stays inside the sbt console when we press "ctrl-c" while a Flink programme executes with "run" or "runMain"
Compile / run / fork := true
Global / cancelable := true

// exclude Scala library from assembly
assembly / assemblyOption  := (assembly / assemblyOption).value.copy(includeScala = false)

