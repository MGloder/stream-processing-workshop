package com.machinedoll.projectdemo.jobs

import com.machinedoll.projectdemo.source.{GDELTLocalSource, PravegaSource}
import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.slf4j.LoggerFactory

object GDELTDataLoader {
  val log = LoggerFactory.getLogger(GDELTDataLoader.getClass)
  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0L))
    val GDELTSource = new GDELTLocalSource(conf)

    log.info("Starting query Reference Link and Save to Pravega platform...")
//    env
//      .addSource(GDELTSource.getSource)
//      .addSink()
//      .name("GDTEL reference link from Pravega")

    env.execute("Load data to Pravega")

  }
}

