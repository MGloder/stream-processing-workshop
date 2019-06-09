package com.machinedoll.projectdemo.jobs

import com.machinedoll.projectdemo.schema.{GDELTRawData, GDGDELTReferenceLink}
import com.machinedoll.projectdemo.sink.PravegaSink
import com.machinedoll.projectdemo.source.{GDELTLinkSource, PravegaSource}
import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.slf4j.LoggerFactory

import scala.io.Source

object GDELTRawDataDownloader {
  def main(args: Array[String]): Unit = {
    val log = LoggerFactory.getLogger(ReferenceLinkDownloader.getClass)
    val conf = ConfigFactory.load
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0L))
    val GDTELSource = new PravegaSource(conf, ParameterTool.fromArgs(args)).getCustomSource()

    log.info("Starting query Reference Link and Save to Pravega platform...")
    env
      .addSource(GDTELSource)
//      .map(RequestRemoteData(_))
      .print()
      .name("GDTEL reference link from Pravega")

    env.execute("Example Pravega")

  }

  def RequestRemoteData(target: GDGDELTReferenceLink): String = {
    // translate target to GDELTRaw Data
    // 1. Download the file
    Source
      .fromURL(target.value)
      .mkString
  }
}
