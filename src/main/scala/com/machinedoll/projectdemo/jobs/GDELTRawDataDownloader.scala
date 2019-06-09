package com.machinedoll.projectdemo.jobs

import java.io.File
import java.net.URL

import com.machinedoll.projectdemo.schema.GDELTReferenceLink
import com.machinedoll.projectdemo.source.PravegaSource
import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.slf4j.LoggerFactory

import scala.sys.process._

object GDELTRawDataDownloader {
  val log = LoggerFactory.getLogger(ReferenceLinkDownloader.getClass)
  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0L))
    val GDTELSource = new PravegaSource(conf, ParameterTool.fromArgs(args)).getCustomSource()
//    val GDETLSink = new PravegaSink(conf, ParameterTool.fromArgs(args)).getCustomSink()
    log.info("Starting query Reference Link and Save to Pravega platform...")
    env
      .addSource(GDTELSource)
      .map(requestRemoteData _)
//      .addSink(GDETLSink)
      .name("GDTEL reference link from Pravega")

    env.execute("Example Pravega")

  }

  def requestRemoteData(target: GDELTReferenceLink): String = {
    //TODO need wait for the completion of the tmp file
    log.info("Download starting...")
    val filename = new File(target.url).getName
    val tmpFile = new File(s"/tmp/${filename}")
    val downloader = new URL(target.url) #> tmpFile
    downloader.run()
    tmpFile.getAbsolutePath
  }
}
