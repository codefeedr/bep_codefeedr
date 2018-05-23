package org.codefeedr.plugins.travis

import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.scala.{AsyncDataStream, DataStream, _}
import org.apache.flink.streaming.api.scala.async.{AsyncFunction, ResultFuture}

import scala.concurrent.{ExecutionContext, Future}
import ExecutionContext.Implicits.global
import org.codefeedr.pipeline.{Pipeline, TransformStage}
import org.codefeedr.plugins.github.GitHubProtocol.PushEvent

class TravisPushEventBuildInfoTransformStage(capacity: Int = 100) extends TransformStage[PushEvent, TravisBuild]{

  var travis: TravisService = _

  override def setUp(pipeline: Pipeline): Unit = {
    super.setUp(pipeline)
    travis = new TravisService(pipeline.keyManager)
  }

  override def transform(source: DataStream[PushEvent]): DataStream[TravisBuild] = {
    AsyncDataStream.unorderedWait(
      source,
      new TravisBuildStatusRequest(travis),
      20,
      TimeUnit.MINUTES,
      capacity)
  }

}

private class TravisBuildStatusRequest(travis: TravisService) extends AsyncFunction[PushEvent, TravisBuild] {

  override def asyncInvoke(input: PushEvent, resultFuture: ResultFuture[TravisBuild]): Unit = {
    // If there are no commits in the push then there will be no build
    if (input.payload.commits.isEmpty) return

    val repoOwner = input.repo.name.split('/')(0)
    val repoName = input.repo.name.split('/')(1)
    val branchName = input.payload.ref.replace("refs/heads/", "")
    val commitSHA = input.payload.commits.last.sha
    val pushDate = input.created_at

    val futureResultBuild: Future[TravisBuild] =
      new TravisBuildCollector(repoOwner, repoName, branchName, commitSHA, pushDate, travis).requestFinishedBuild()

    futureResultBuild.onSuccess {
      case result: TravisBuild => resultFuture.complete(Iterable(result))
    }
  }
}
