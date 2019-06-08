package com.machinedoll.projectdemo.jobs

import com.machinedoll.projectdemo.sink.PravegaSink
import com.machinedoll.projectdemo.source.GDELTLinkSource
import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.slf4j.LoggerFactory

object ReferenceLinkDownloader {
  def main(args: Array[String]): Unit = {
    val log = LoggerFactory.getLogger(ReferenceLinkDownloader.getClass)
//    Setting up flink env
    val conf = ConfigFactory.load
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0L))

//    Setting up Pravega env
    val GDETLSink = new PravegaSink(conf, ParameterTool.fromArgs(args)).getCustomSink()
    val GDETLSource = new GDELTLinkSource(conf)

    log.info("Starting query Reference Link and Save to Pravega platform...")
    env
      .addSource(GDETLSource.getSource)
      .addSink(GDETLSink)
      .name("GDETL Reference Link")

    env.execute("Example Pravega")
  }
}
