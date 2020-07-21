package org.codefeedr.plugins.npm.stages

import java.util.Date

import com.sksamuel.avro4s.{AvroSchema, SchemaFor}
import org.apache.avro.Schema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.DataStream
import org.codefeedr.pipeline.Context
import org.codefeedr.stages.InputStage
import org.codefeedr.plugins.npm.operators.{NpmReleasesSource, NpmSourceConfig}
import org.codefeedr.plugins.npm.protocol.Protocol.NpmRelease

import scala.language.higherKinds

/**
  * Fetches real-time releases from Npm.
  *
  * @author Roald van der Heijden
  * Date: 2019-12-01 (YYYY-MM-DD)
  */
class NpmReleasesStage(stageId: String = "npm_releases_min",
                       sourceConfig: NpmSourceConfig = NpmSourceConfig())
    extends InputStage[NpmRelease](Some(stageId)) {

  /**
    * Fetches [[NpmRelease]] from real-time Npm feed.
    *
    * @param context The context to add the source to.
    * @return The stream of type [[NpmRelease]].
    */
  override def main(context: Context): DataStream[NpmRelease] = {
    implicit val typeInfo: TypeInformation[NpmRelease] =
      TypeInformation.of(classOf[NpmRelease])
    context.env
      .addSource(new NpmReleasesSource(sourceConfig))(typeInfo)
  }

  override def getSchema: Schema = {
    implicit val dateSchemaFor: AnyRef with SchemaFor[Date] =
      SchemaFor[Date](Schema.create(Schema.Type.STRING))
    AvroSchema[NpmRelease]
  }
}
