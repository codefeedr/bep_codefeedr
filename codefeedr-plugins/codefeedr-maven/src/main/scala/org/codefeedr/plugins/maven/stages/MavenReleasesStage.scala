package org.codefeedr.plugins.maven.stages

import com.sksamuel.avro4s.AvroSchema
import org.apache.avro.Schema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.DataStream
import org.codefeedr.pipeline.Context
import org.codefeedr.stages.InputStage
import org.codefeedr.plugins.maven.protocol.Protocol.MavenRelease
import org.codefeedr.plugins.maven.operators.{
  MavenReleasesSource,
  MavenSourceConfig
}
import org.codefeedr.stages.utilities.DefaultTypeMapper._

import scala.language.higherKinds

/** Fetches real-time releases from Maven. */
class MavenReleasesStage(stageId: String = "maven_releases_min",
                         sourceConfig: MavenSourceConfig =
                           MavenSourceConfig(1000, -1))
    extends InputStage[MavenRelease](Some(stageId)) {

  /** Fetches [[MavenRelease]] from real-time Maven feed.
    *
    * @param context The context to add the source to.
    * @return The stream of type [[MavenRelease]].
    */
  override def main(context: Context): DataStream[MavenRelease] = {
    implicit val typeInfo: TypeInformation[MavenRelease] =
      TypeInformation.of(classOf[MavenRelease])
    context.env
      .addSource(new MavenReleasesSource(sourceConfig))(typeInfo)
  }

  override def getSchema: Schema = {
    implicit val MavenSchema: DateSchemaFor = new DateSchemaFor(true)
    AvroSchema[MavenRelease]
  }
}
