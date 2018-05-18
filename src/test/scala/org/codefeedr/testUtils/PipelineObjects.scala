package org.codefeedr.testUtils

import org.apache.flink.api.common.JobID
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.api.scala._
import org.apache.flink.runtime.client.JobExecutionException
import org.apache.flink.streaming.api.functions.sink.{PrintSinkFunction, RichSinkFunction, SinkFunction}
import org.codefeedr.pipeline._

case class StringType(value: String) extends PipelineItem

//This will be thrown after the print sink received x elements.
final case class JobFinishedException() extends JobExecutionException(new JobID(), "Job is finished.")

final case class CodeHitException() extends RuntimeException

//a simple test source which generates some StringType messages
class SimpleSourcePipelineObject(attributes: StageAttributes = StageAttributes()) extends PipelineObject[NoType, StringType](attributes) {
  override def transform(source: DataStream[NoType]): DataStream[StringType] = {
    pipeline.
      environment.addSource {
      new RichSourceFunction[StringType] {
        override def run(ctx: SourceFunction.SourceContext[StringType]): Unit =  {
          val words : Array[String] = Array("simple1", "simple2", "simple3")
          words.foreach(x => ctx.collect(StringType(x)))
          ctx.close()
        }

        override def cancel(): Unit = {}
      }
    }
  }

}

//simply transforms from x => x
class SimpleTransformPipelineObject extends PipelineObject[StringType, StringType] {
  override def transform(source: DataStream[StringType]): DataStream[StringType] = source
}


//simple sink which prints the elements and stops after #elements
class SimpleSinkPipelineObject(elements : Int = -1) extends OutputStage[StringType] {
  override def main(source: DataStream[StringType]): Unit = {
    source.addSink(new PrintSinkElements(elements)).setParallelism(1)
  }
}

//keeps track of the amount of prints and stops after #elements
class PrintSinkElements(elements : Int) extends PrintSinkFunction[StringType] {
  var count = 0

  def invoke(value: StringType, context: SinkFunction.Context[_]): Unit = {
    super.invoke(value)
    count += 1

    if (elements != -1 && count >= elements) {
      throw new JobFinishedException()
    }
  }
}

class HitObjectTest extends PipelineObject[NoType, NoType] {
  override def transform(source: DataStream[NoType]): DataStream[NoType] = {
    throw CodeHitException()
  }
}

class FlinkCrashObjectTest extends PipelineObject[NoType, NoType] {
  override def transform(source: DataStream[NoType]): DataStream[NoType] = {
    pipeline.environment.fromCollection[String](Seq("a", "b"))
      .map { a => throw CodeHitException() }
  }
}


