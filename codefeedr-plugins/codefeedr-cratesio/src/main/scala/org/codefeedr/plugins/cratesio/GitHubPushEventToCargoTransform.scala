package org.codefeedr.plugins.cratesio

import com.twitter.chill.Externalizer
import org.apache.flink.streaming.api.scala.DataStream
import org.codefeedr.plugins.cratesio.CargoProtocol.CrateInfo
import org.codefeedr.plugins.github.GitHubProtocol.PushEvent
import org.apache.flink.api.scala._
import org.codefeedr.stages.{StageAttributes, TransformStage}

class GitHubPushEventToCargoTransform(stageAttributes : StageAttributes = StageAttributes()) extends TransformStage[PushEvent, CrateInfo](stageAttributes) {


  val defaultRepo = "rust-lang/crates.io-index"

  /**
    * Transfrom PushEvents into CrateInfo.s
    *
    * @param source a push_event stream.
    * @return a stream of crateinfos.
    */
  override def transform(source: DataStream[PushEvent]): DataStream[CrateInfo] = {
    source.
      filter(e => e.repo.name == defaultRepo).
      flatMap(e => e.payload.commits.map(c => new CratesIOAPIService().extractCrateFromCommitMsg(c.message))).
      filter (t => t.isSuccess).
      map(crate => new CratesIOAPIService().crateInfo(crate.get)).
      filter(crate => crate.isDefined).
      map(crate => crate.get)
  }
}
