import sbt.Credentials
import sbt.Keys.{credentials, name}

name := "codefeedr"
ThisBuild / version := "0.1-SNAPSHOT"
ThisBuild / organization := "org.codefeedr"
ThisBuild / scalaVersion := "2.11.11"

parallelExecution in Test := false

/// PROJECTS

val projectPrefix = "codefeedr-"

lazy val root = (project in file("."))
  .settings(settings)
  .aggregate(
    core,
    pluginRss,
    pluginMongodb,
    pluginElasticSearch,
    pluginGitHub,
    pluginWeblogs,
    pluginCratesIO,
    pluginTravis,
    pluginTwitter
  )

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

      // RabbitMQBuffer
      dependencies.flinkRabbitMQ,

      // RedisKeyManager
      dependencies.redis,

      // Schema exposure
      dependencies.zookeeper,

      // BSON serialization
      dependencies.mongo,

      // Kryo serialization
      dependencies.kryoChill,

      // Avro schema exposure
      dependencies.avro
    )
  )

lazy val pluginRss = (project in file("codefeedr-plugins/codefeedr-rss"))
  .settings(
    name := projectPrefix + "rss",
    settings,
    assemblySettings,
    libraryDependencies ++= commonDependencies ++ Seq(
      dependencies.httpj
    )
  )
  .dependsOn(
    core
  )

lazy val pluginMongodb = (project in file("codefeedr-plugins/codefeedr-mongodb"))
  .settings(
    name := projectPrefix + "mongodb",
    settings,
    assemblySettings,
    libraryDependencies ++= commonDependencies ++ Seq(
      dependencies.mongo
    )
  )
  .dependsOn(
    core
  )

lazy val pluginElasticSearch = (project in file("codefeedr-plugins/codefeedr-elasticsearch"))
  .settings(
    name := projectPrefix + "elasticsearch",
    settings,
    assemblySettings,
    libraryDependencies ++= commonDependencies ++ Seq(
      dependencies.flinkElasticSearch
    )
  )
  .dependsOn(
    core
  )

lazy val pluginGitHub = (project in file("codefeedr-plugins/codefeedr-github"))
  .settings(
    name := projectPrefix + "github",
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
  .dependsOn(
    core
  )

lazy val pluginTravis = (project in file("codefeedr-plugins/codefeedr-travis"))
  .settings(
    name := projectPrefix + "travis",
    settings,
    assemblySettings,
    libraryDependencies ++= commonDependencies ++ Seq(
      dependencies.httpj
    )
  )
  .dependsOn(
    core,
    pluginGitHub
  )

lazy val pluginWeblogs = (project in file("codefeedr-plugins/codefeedr-weblogs"))
  .settings(
    name := projectPrefix + "weblogs",
    settings,
    assemblySettings,
    libraryDependencies ++= commonDependencies ++ Seq(
    )
  )
  .dependsOn(
    core
  )

lazy val pluginCratesIO = (project in file("codefeedr-plugins/codefeedr-cratesio"))
  .settings(
    name := projectPrefix + "cratesio",
    settings,
    assemblySettings,
    libraryDependencies ++= commonDependencies ++ Seq(
      dependencies.httpj,
      dependencies.json4s,
      dependencies.jackson,
      dependencies.json4sExt
    )
  )
  .dependsOn(
    core, pluginGitHub
  )

lazy val pluginTwitter = (project in file("codefeedr-plugins/codefeedr-twitter"))
  .settings(
    name := projectPrefix + "twitter",
    settings,
    assemblySettings,
    libraryDependencies ++= commonDependencies ++ Seq(
      dependencies.twitter
    ),
    dependencyOverrides ++= Seq( //override json4s dependencies
      "org.json4s" %% "json4s-scalap" % "3.5.3",
      "org.json4s" %% "json4s-jackson" % "3.5.3",
      "org.json4s" %% "json4s-ext" % "3.5.3"
    )
  )
  .dependsOn(
    core
  )

lazy val dependencies =
  new {
    val flinkVersion       = "1.7.0"
    val json4sVersion      = "3.6.0-M2"
    val log4jVersion       = "2.11.0"
    val log4jScalaVersion  = "11.0"


    val loggingApi         = "org.apache.logging.log4j"   % "log4j-api"                      % log4jVersion
    val loggingCore        = "org.apache.logging.log4j"   % "log4j-core"                     % log4jVersion      % Runtime
    val loggingScala       = "org.apache.logging.log4j"  %% "log4j-api-scala"                % log4jScalaVersion

    val flink              = "org.apache.flink"          %% "flink-scala"                    % flinkVersion      % Provided
    val flinkStreaming     = "org.apache.flink"          %% "flink-streaming-scala"          % flinkVersion      % Provided
    val flinkKafka         = "org.apache.flink"          %% "flink-connector-kafka-0.11"     % flinkVersion
    val flinkRuntimeWeb    = "org.apache.flink"          %% "flink-runtime-web"              % flinkVersion      % Provided
    val flinkElasticSearch = "org.apache.flink"          %% "flink-connector-elasticsearch6" % flinkVersion
    val flinkRabbitMQ      = "org.apache.flink"          %% "flink-connector-rabbitmq"       % flinkVersion

    val redis              = "net.debasishg"             %% "redisclient"                    % "3.6"
    val kafkaClient        = "org.apache.kafka"           % "kafka-clients"                  % "1.0.0"
    val zookeeper          = "org.apache.zookeeper"       % "zookeeper"                      % "3.4.9"

    val json4s             = "org.json4s"                %% "json4s-scalap"                  % json4sVersion
    val jackson            = "org.json4s"                %% "json4s-jackson"                 % json4sVersion
    val json4sExt          = "org.json4s"                %% "json4s-ext"                     % json4sVersion

    val mongo              = "org.mongodb.scala"         %% "mongo-scala-driver"             % "2.3.0"

    val httpj              = "org.scalaj"                %% "scalaj-http"                    % "2.4.0"

    val kryoChill          = "com.twitter"               %% "chill"                          % "0.9.1"

    val scalactic          = "org.scalactic"             %% "scalactic"                      % "3.0.1"           % Test
    val scalatest          = "org.scalatest"             %% "scalatest"                      % "3.0.1"           % Test
    val scalamock          = "org.scalamock"             %% "scalamock"                      % "4.1.0"           % Test
    val mockito            = "org.mockito"                % "mockito-all"                    % "1.10.19"         % Test

    val avro               = "org.apache.avro"            % "avro"                           % "1.8.2"
    val twitter            = "com.danielasfregola"        %% "twitter4s"                     % "5.5"
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
//  organization := "org.codefeedr",
//  version := "0.1.0-SNAPSHOT",
//  scalaVersion := "2.11.11",
  test in assembly := {},
  scalacOptions ++= compilerOptions,
  resolvers ++= Seq(
    "confluent"                               at "http://packages.confluent.io/maven/",
    "Apache Development Snapshot Repository"  at "https://repository.apache.org/content/repositories/snapshots/",
    "Artima Maven Repository"                 at "http://repo.artima.com/releases",
    Resolver.mavenLocal
  ),

  // Deploying. NOTE: when releasing, do not add the timestamp
  publishTo := Some("Artifactory Realm" at "http://codefeedr.joskuijpers.nl:8081/artifactory/sbt-dev-local;build.timestamp=" + new java.util.Date().getTime),
  credentials += Credentials("Artifactory Realm", "codefeedr.joskuijpers.nl", sys.env.getOrElse("ARTIFACTORY_USERNAME", ""), sys.env.getOrElse("ARTIFACTORY_PASSWORD", ""))
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
