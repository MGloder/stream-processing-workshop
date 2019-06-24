package com.machinedoll.projectdemo.jobs.option4

import com.machinedoll.projectdemo.jobs.gdelt.source.GDELTSource
import com.typesafe.config.ConfigFactory
import org.apache.commons.logging.LogFactory
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

object AllExportLinkLoader {
  val Log = LogFactory.getLog(AllExportLinkLoader.getClass)

  def main(args: Array[String]): Unit = {
    Log.info("Starting Download Export Data...")

    val conf = ConfigFactory.load

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0L))

    val tempFolder = "/Users/xyan/Data/GDELT"
    val exportSource = new GDELTSource(conf).getAllExportSource(tempFolder)

    env.addSource(exportSource)
      .print()

    env.execute("Downloading all links from remote")
  }
}
