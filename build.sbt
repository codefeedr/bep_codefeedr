// import sbt.Keys.libraryDependencies

resolvers in ThisBuild ++= Seq(
  "confluent" at "http://packages.confluent.io/maven/",
  "Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/",
  "Artima Maven Repository" at "http://repo.artima.com/releases",
  Resolver.mavenLocal
)

name := "CodeFeedr"
version := "0.1-SNAPSHOT"
organization := "org.codefeedr"

ThisBuild / scalaVersion := "2.11.11"

parallelExecution in Test := false

val flinkVersion = "1.4.2"
val flinkDependencies = Seq(
  "org.apache.flink" %% "flink-scala" % flinkVersion % "compile",
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "compile",
  "org.apache.flink" %% "flink-connector-kafka-0.11" % flinkVersion % "compile",
  // https://mvnrepository.com/artifact/org.apache.flink/flink-avro
  "org.apache.flink" % "flink-avro" % flinkVersion,
  "org.apache.flink" % "flink-runtime-web_2.11" % flinkVersion,

  "org.apache.flink" % "flink-connector-elasticsearch5_2.11" % flinkVersion

  //  "org.apache.flink" %% "flink-table" % flinkVersion % "provided"
)

val coreDependencies = Seq(
  "org.apache.zookeeper" % "zookeeper" % "3.4.9",
  // "org.mockito" % "mockito-core" % "2.13.0" % "test",
  "org.json4s" % "json4s-scalap_2.11" % "3.6.0-M2",
  "org.json4s" % "json4s-jackson_2.11" % "3.6.0-M2",

  "net.debasishg" %% "redisclient" % "3.6",

   "org.scalactic" %% "scalactic" % "3.0.1",
   "org.scalatest" %% "scalatest" % "3.0.1" % "test",

  "org.mongodb.scala" %% "mongo-scala-driver" % "2.3.0",

  "org.apache.logging.log4j" % "log4j-api" % "2.11.0",
  "org.apache.logging.log4j" % "log4j-core" % "2.11.0",

  "org.apache.kafka" % "kafka-clients" % "1.0.0",
  "io.confluent" % "kafka-avro-serializer" % "4.0.0",
  "me.lyh" %% "shapeless-datatype-avro" % "0.1.9",
  "org.json4s" % "json4s-scalap_2.11" % "3.6.0-M2",
  "org.json4s" % "json4s-jackson_2.11" % "3.6.0-M2",

  // "com.jsuereth" %% "scala-arm" % "2.0",
  // "org.scala-lang.modules" % "scala-java8-compat_2.11" % "0.8.0",
  // "org.scala-lang.modules" %% "scala-async" % "0.9.7",
  // "io.reactivex" %% "rxscala" % "0.26.5"
  "org.json4s" %% "json4s-ext" % "3.6.0-M3",

  // https://mvnrepository.com/artifact/org.scalaj/scalaj-http
  "org.scalaj" %% "scalaj-http" % "2.4.0",
  "org.json4s" %% "json4s-ext" % "3.6.0-M3",

  "com.chuusai" %% "shapeless" % "2.3.3",

  "org.scalamock" % "scalamock_2.11" % "4.1.0" % "test",
  "org.mockito" % "mockito-all" % "1.10.19" % "test"
)


lazy val root = (project in file("."))
  .settings(
    libraryDependencies ++= coreDependencies,
    libraryDependencies ++= flinkDependencies,
    libraryDependencies ~= { _.map(_.exclude("org.slf4j", "slf4j-log4j12")) }
  )

assembly / mainClass := Some("org.codefeedr.Main")

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

