package org.codefeedr.plugins.travis

import org.apache.flink.streaming.api.scala._
import org.codefeedr.keymanager.StaticKeyManager
import org.codefeedr.pipeline.{PipelineBuilder, PipelineItem}
import org.codefeedr.plugins.github.GitHubProtocol.PushEvent
import org.codefeedr.plugins.github.stages.{GitHubEventToPushEvent, GitHubEventsInput}
import org.codefeedr.plugins.travis.TravisProtocol.{PushEventFromActiveTravisRepo, TravisBuild}
import org.codefeedr.plugins.travis.stages.{TravisFilterActiveReposTransformStage, TravisPushEventBuildInfoTransformStage}

import scala.io.Source

object TravisTransformStageTest {

  def main(args: Array[String]): Unit = {

    val pipeline = new PipelineBuilder()
      .setKeyManager(new StaticKeyManager(Map("travis" -> "su_9TrVO1Tbbti1UoG0Z_w",
        "events_source" -> Source.fromInputStream(getClass.getResourceAsStream("/github_api_key")).getLines().next())))

      .append(new GitHubEventsInput())
      .append(new GitHubEventToPushEvent())
      .append(new TravisFilterActiveReposTransformStage)
      .append{x: DataStream[PushEventFromActiveTravisRepo] =>
        x.map{event =>
          println(event.pushEventItem.repo.name,
            event.pushEventItem.payload.ref.replace("refs/heads/", ""),
            event.pushEventItem.payload.head)
          event
        }
      }
      .append(new TravisPushEventBuildInfoTransformStage(10))
      .append{x: DataStream[TravisBuild] =>
        x.map(x => (x.repository.slug, x.state, x.duration.getOrElse(0), x.branch.name, x.commit.sha)).print()
      }

      .build()
//      .startMock()

  }

}

class TravisTransformStageTest {

}
