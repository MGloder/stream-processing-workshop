package com.machinedoll.projectdemo.jobs

import java.io.File
import java.net.URL
import java.nio.file.Files

import com.machinedoll.projectdemo.schema.{ExampleData, GDELTRawData, GDELTReferenceLink}
import com.machinedoll.projectdemo.sink.PravegaSink
import com.machinedoll.projectdemo.source.{GDELTLinkSource, PravegaSource}
import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.slf4j.LoggerFactory

import scala.io.Source
import scala.util.Try
import sys.process._

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
      .map(RequestRemoteData(_))
      .name("GDTEL reference link from Pravega")

    env.execute("Example Pravega")

  }

  def unzipFile(tmpFile: File): Unit = {
    import java.io.IOException
    import java.util.zip.ZipEntry
    import java.util.zip.ZipInputStream
    try {
      val zipInputStream = new ZipInputStream(Files.newInputStream(tmpFile.toPath))
      try {
        var entry: ZipEntry = null
        while ((entry = zipInputStream.getNextEntry) != null) {
          val toPath = tmpFile.toPath.getParent.resolve(entry.getName)
          if (entry.isDirectory) Files.createDirectory(toPath)
          else Files.copy(zipInputStream, toPath)
        }
      } catch {
        case _ => println("an error occured!")
      } finally if (zipInputStream != null) zipInputStream.close()
    }
  }

  def RequestRemoteData(target: GDELTReferenceLink): String = {
    println("Download starting...")
    val filename = new File(target.url).getName
    val tmpFile = new File(s"/tmp/${filename}")
    val downloader = new URL(target.url) #> tmpFile
    println("Download started")
    downloader.run()
    s"download complete"
  }
}
  //  def RequestRemoteData(target: GDELTReferenceLink): DataStream[GDELTRawData] = {
  //    import sys.process._
  //    import java.net.URL
  //    import java.io.File
  //     translate target to GDELTRaw Data
  //     1. Download the file with java.net.URL
  //     2. unzip file
  //     3. load csv
  //     4. rm zip file in thread
  //     5. rm csv
  //     6. return dataset
  //    println("Downloading zip file")
  //    val filename = new File(target.url).getName
  //    val downloader = new Thread(new Runnable {
  //      override def run(): Unit = new URL(target.url) #> new File(s"/tmp/${filename}")
  //    })
  //    downloader.run()
  //    downloader.join()
  //    val targetZip = new File(s"/tmp/${filename}")
  //    while(true){
  //      Try {
  //        if(targetZip.exists){
  //          print("File exist! trying to unzip file")
  //        }
  //      }
  //    }
  //    ???
  //  }
