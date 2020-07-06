package org.codefeedr.plugins.maven.operators

import org.apache.flink.streaming.api.functions.async.{
  ResultFuture,
  RichAsyncFunction
}
import org.codefeedr.plugins.maven.protocol.Protocol.{
  MavenProject,
  MavenRelease,
  MavenReleaseExt
}
import org.codefeedr.plugins.maven.util.MavenService

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}
import collection.JavaConverters._

/** Retrieves a project related to a release asynchronously. */
class RetrieveProjectAsync
    extends RichAsyncFunction[MavenRelease, MavenReleaseExt] {

  /** Retrieve the execution context lazily. */
  implicit lazy val executor: ExecutionContext = ExecutionContext.global

  /** Async retrieves the project belonging to the release.
    *
    * @param input        the release.
    * @param resultFuture the future to add the project to.
    */
  override def asyncInvoke(
      input: MavenRelease,
      resultFuture: ResultFuture[MavenReleaseExt]): Unit = {

    val projectName = transformProjectName(input)

    /** Retrieve the project in a Future. */
    val requestProject: Future[Option[MavenProject]] = Future(
      MavenService.getProject(projectName))

    /** Collects the result. */
    requestProject.onComplete {
      case Success(result: Option[MavenProject]) =>
        if (result.isDefined) {
          resultFuture.complete(
            List(
              MavenReleaseExt(input.title,
                              input.link,
                              input.description,
                              input.pubDate,
                              input.guid,
                              result.get)).asJava)
        } else resultFuture.complete(List().asJava)
      case Failure(e) =>
        resultFuture.complete(List().asJava)
        e.printStackTrace()
    }
  }

  /**
    * Transform the title of a project to be retrieved by the MavenService
    * @param input The project of which the name needs to be transformed
    * @return the transformed project name
    */
  private def transformProjectName(input: MavenRelease): String = {
    val splitTitle = input.title.split(" ")
    val org = splitTitle(0).replace(".", "/").replace(":", "/")
    val name = splitTitle(1).replace(" ", "/")
    val version = splitTitle(2)
    org + name + "/" + version + "/" + name + "-" + version + ".pom"
  }

  /** If we retrieve a time-out, then we just complete the future with an empty list. */
  override def timeout(input: MavenRelease,
                       resultFuture: ResultFuture[MavenReleaseExt]): Unit =
    resultFuture.complete(List().asJava)
}
