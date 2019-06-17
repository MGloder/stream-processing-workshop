package com.machinedoll.projectdemo.jobs.gdelt.source

import com.typesafe.config.ConfigFactory
import org.apache.commons.logging.LogFactory
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

object ExportData {
  def LOG = LogFactory.getLog(ExportData.getClass)

  def main(args: Array[String]): Unit = {
    LOG.info("Starting Download Export Data...")

    val conf = ConfigFactory.load()

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0L))

    val exportSource = new GDELTSource(conf).getExportSource()

    env
      .addSource(exportSource)
      .print()

    env.execute("Download Export Data")
  }
}
