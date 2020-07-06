package org.codefeedr.plugins.cargo.stages

import com.sksamuel.avro4s.AvroSchema
import org.apache.avro.Schema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.DataStream
import org.codefeedr.pipeline.Context
import org.codefeedr.stages.InputStage
import org.codefeedr.plugins.cargo.operators.{
  CargoReleasesSource,
  CargoSourceConfig
}
import org.codefeedr.plugins.cargo.protocol.Protocol.CrateRelease

import scala.language.higherKinds

/** fetches real-time releases from Cargo */
class CargoReleasesStage(stageId: String = "cargo_releases_min",
                         sourceConfig: CargoSourceConfig =
                           CargoSourceConfig(500, -1))
    extends InputStage[CrateRelease](Some(stageId)) {

  /** Fetches [[CrateRelease]] from real-time Cargo feed.
    *
    * @param context The context to add the source to.
    * @return The stream of type [[CrateRelease]].
    */
  override def main(context: Context): DataStream[CrateRelease] = {
    implicit val typeInfo: TypeInformation[CrateRelease] =
      TypeInformation.of(classOf[CrateRelease])
    context.env
      .addSource(new CargoReleasesSource(sourceConfig))(typeInfo)
  }

  override def getSchema: Schema = AvroSchema[CrateRelease]
}
