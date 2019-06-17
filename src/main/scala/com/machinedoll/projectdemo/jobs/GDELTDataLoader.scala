package com.machinedoll.projectdemo.jobs

import java.util.Date

import com.machinedoll.projectdemo.schema.FileInfo
import com.machinedoll.projectdemo.utils.Pravega
import io.pravega.client.stream.{ScalingPolicy, StreamConfiguration}
import io.pravega.connectors.flink.serialization.PravegaSerialization
import io.pravega.connectors.flink.{FlinkPravegaReader, PravegaConfig}
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

object GDELTDataLoader {
  private val LOG = LoggerFactory.getLogger("GDELTDataLoader")

  // The application reads data from specified Pravega stream and once every 10 seconds
  // prints the distinct words and counts from the previous 10 seconds.

  // Application parameters
  //   stream - default examples/wordcount
  //   controller - default tcp://127.0.0.1:9090

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    LOG.info("Starting WordCountReader...")
    // initialize the parameter utility tool in order to retrieve input parameters
//    val params = ParameterTool.fromArgs(args)
//    val pravegaConfig = PravegaConfig.fromParams(params)
//      .withDefaultScope("example")
    // create the Pravega input stream (if necessary)
//    val stream = Pravega.createStream(pravegaConfig, Constants.DEFAULT_STREAM, StreamConfiguration.builder.scalingPolicy(ScalingPolicy.fixed(1)).build)
    // initialize the Flink execution environment
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // create the Pravega source to read a stream of text
//    val source = FlinkPravegaReader.builder[FileInfo].withPravegaConfig(pravegaConfig).forStream(stream).withDeserializationSchema(PravegaSerialization.deserializationFor(classOf[FileInfo])).build
    // count each word over a 10 second time period
//    val dataStream = env.addSource(source).name("Pravega Stream")
//    dataStream.print
//    env.execute("GDELTDataLoader")
//    LOG.info("Ending GDELTDataLoader...")
  }

  // split data into word by space
  private class Splitter extends FlatMapFunction[String, FileInfo] {
    @throws[Exception]
    override def flatMap(line: String, out: Collector[FileInfo]): Unit = {
      for (word <- line.split(" ")) {
        out.collect(new FileInfo("1", "1", new Date().toString))
      }
    }
  }

}

