package org.codefeedr.plugins.travis

import java.time.LocalDateTime

import org.apache.flink.runtime.concurrent.Executors

import scala.concurrent.{ExecutionContext, Future}
import ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

object TravisBuildCollector {

  def main(args: Array[String]): Unit = {
//    implicit lazy val executor: ExecutionContext = ExecutionContext.fromExecutor(Executors.directExecutor())

    val collector = new TravisBuildCollector("joskuijpers", "bep_codefeedr", "develop", "bb5cc7e5a19a84d71d627d202d44ea81598d9a68", LocalDateTime.MIN)

    val result: Future[Option[TravisBuild]] = collector.requestFinishedBuild()
    result.onComplete {
      case Success(Some(x)) => println("Succeeded", x.commit.message.replace('\n', ' '))
      case Success(None) => println("Build not found")
      case Failure(e) => e.printStackTrace()
    }

    while(true) Thread.sleep(1000)

  }
}


//TODO only give GithubPushEvent as constructor argument
class TravisBuildCollector(repoOwner: String, repoName: String, branchName: String, pushCommitSha: String, pushDate: LocalDateTime) {
  private val travis: TravisService = new TravisService()

  private var willNotBuild: Boolean = false
  private var minimumCreationDate: LocalDateTime = pushDate
  private var build: Option[TravisBuild] = None

  def requestFinishedBuild(): Future[Option[TravisBuild]] = Future {
    while (!isReady) {
      println("Requesting build")
      build = requestBuild()
      println("Status of the build: " + (if(build.nonEmpty) build.get.state else "not found yet"))
      if (!isReady) {
        println("Not ready yet now sleeping for 10 seconds")
        Thread.sleep(10000)
      }
    }
    println("Build is found and is finished. Now returning...")
    build
  }

  private def isReady: Boolean = {
    if (willNotBuild) {
      true
    }
    else if (build.nonEmpty) {
      val state = build.get.state
      state == "passed" || state == "failed"
    }
    else {
      false
    }
  }

  private def requestBuild(): Option[TravisBuild] = build match {
    case None => requestUnknownBuild()
    case Some(_) => requestKnownBuild()
  }

  def requestKnownBuild(): Option[TravisBuild] = {
    assert(build.nonEmpty)
    Some(travis.getBuild(build.get.id))
  }

  def requestUnknownBuild(): Option[TravisBuild] = {

    var newestBuildDate: Option[LocalDateTime] = None

    var builds: TravisBuilds = new TravisService().getTravisBuilds(repoOwner, repoName, branchName, limit = 5)
    if (builds.builds.nonEmpty) {
      if(!builds.builds.head.repository.active.orElse(Some(false)).get) {
        willNotBuild = true
        return None
      }
      newestBuildDate = builds.builds.head.started_at
    }

    do {
      val buildIterator = builds.builds.iterator

      while (buildIterator.hasNext) {
        val x = buildIterator.next()
        if (x.started_at.getOrElse(LocalDateTime.MAX).isBefore(minimumCreationDate)) {
          return None
        }
        else if (x.commit.sha == pushCommitSha) {
          return Some(x)
        }
      }

      val offset = if (builds == null ) 0 else builds.`@pagination`.next.offset
      builds = new TravisService().getTravisBuilds(repoOwner, repoName, branchName, offset, limit = 5)

    } while (!builds.`@pagination`.is_last)

    None
  }

}
