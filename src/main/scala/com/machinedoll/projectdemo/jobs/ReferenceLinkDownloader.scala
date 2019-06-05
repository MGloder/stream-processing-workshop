package com.machinedoll.projectdemo.jobs

import com.machinedoll.projectdemo.sink.PravegaSink
import com.machinedoll.projectdemo.source.GDELTLinkSource
import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

object ReferenceLinkDownloader {
  def main(args: Array[String]): Unit = {
    val checkPointIntervalMillis: Long = 60 * 15 * 1000
    val conf = ConfigFactory.load

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(checkPointIntervalMillis, CheckpointingMode.EXACTLY_ONCE)
        .setParallelism(1)

    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0L))

    val GDETLSink = new PravegaSink(conf, ParameterTool.fromArgs(args)).getCustomSink()

    val GDETLSource = new GDELTLinkSource(conf)


    env
      .addSource(GDETLSource.getSource)
      .addSink(GDETLSink)
      .setParallelism(2)


    env.execute("Example Pravega")

  }
}
