package org.codefeedr.plugins.npm.operators

import org.apache.flink.streaming.api.functions.async.{
  ResultFuture,
  RichAsyncFunction
}
import org.codefeedr.plugins.npm.protocol.Protocol.{NpmRelease, NpmReleaseExt}
import org.codefeedr.plugins.npm.util.NpmService
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}
import collection.JavaConverters._

/**
  * Retrieves a project related to a release asynchronously.
  *
  * @author Roald van der Heijden
  * Date: 2019-12-01 (YYYY-MM-DD)
  */
class RetrieveProjectAsync
    extends RichAsyncFunction[NpmRelease, NpmReleaseExt] {

  /**
    * Retrieve the execution context lazily.
    */
  implicit lazy val executor: ExecutionContext = ExecutionContext.global

  /**
    * Async retrieves the project belonging to the release.
    *
    * @param input        the release.
    * @param resultFuture the future to add the project to.
    */
  override def asyncInvoke(input: NpmRelease,
                           resultFuture: ResultFuture[NpmReleaseExt]): Unit = {

    /**
      * transform the title of a project to be retrieved by the NpmService
      */
    val link = input.name

    /**
      * Retrieve the project in a Future.
      */
    val requestProject: Future[Option[NpmReleaseExt]] = Future(
      NpmService.getProject(link))

    /**
      * Collects the result.
      */
    requestProject.onComplete {
      case Success(result: Option[NpmReleaseExt]) =>
        if (result.isDefined) { //If we get None, we return nothing.
          resultFuture.complete(
            List(NpmReleaseExt(input.name,
                               input.retrieveDate,
                               result.get.project)).asJava)
        } else {
          resultFuture.complete(List().asJava)
        }
      case Failure(e) =>
        resultFuture.complete(List().asJava)
        e.printStackTrace()
    }

  }

  /**
    * If we retrieve a time-out, then we just complete the future with an empty list.
    */
  override def timeout(input: NpmRelease,
                       resultFuture: ResultFuture[NpmReleaseExt]): Unit =
    resultFuture.complete(List().asJava)
}
