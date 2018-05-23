package org.codefeedr.plugins.travis

import org.apache.flink.streaming.api.scala._
import org.codefeedr.keymanager.StaticKeyManager
import org.codefeedr.pipeline.{PipelineBuilder, PipelineItem}
import org.codefeedr.plugins.github.GitHubProtocol.PushEvent
import org.codefeedr.plugins.github.stages.{GitHubEventToPushEvent, GitHubEventsInput}

import scala.io.Source

object TravisTransformStageTest {

  def main(args: Array[String]): Unit = {

    val pipeline = new PipelineBuilder()
      .setKeyManager(new StaticKeyManager(Map("travis" -> "su_9TrVO1Tbbti1UoG0Z_w",
        "events_source" -> Source.fromInputStream(getClass.getResourceAsStream("/github_api_key")).getLines().next())))

      .append(new GitHubEventsInput(1))
      .append(new GitHubEventToPushEvent())
      .append{x: DataStream[PushEvent] => x.map{event => println(event.repo); event}}
//      .append(new TravisFilterActiveReposTransformStage)
      .append(new TravisPushEventBuildInfoTransformStage())
      .append{x: DataStream[TravisBuild] =>
        x.map(x => (x.id, x.state, x.repository.name, x.duration)).print()
      }

      .build()
//      .startMock()

  }

}

class TravisTransformStageTest {

}
