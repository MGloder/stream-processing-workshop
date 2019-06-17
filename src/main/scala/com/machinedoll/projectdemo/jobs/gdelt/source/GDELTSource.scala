package com.machinedoll.projectdemo.jobs.gdelt.source

import java.util.Calendar

import com.machinedoll.projectdemo.schema.{Export, GDELTReferenceLink}
import com.typesafe.config.Config
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

import scala.io.Source

class GDELTSource(config: Config) extends Serializable {

  def getExportLink(downloadLink: String): String = {
    val gdeltReferenceInfo = Source
      .fromURL(downloadLink)
      .mkString
      .split("\\\n")
      .toList
      .map(instance => {
        instance.split("\\s").toList
      })
      .map(ref => GDELTReferenceLink(ref(0).toDouble, ref(1), ref(2), Calendar.getInstance.getTimeInMillis.toString))
    gdeltReferenceInfo.head.url
  }

  def getExportZipFile(exportLink: String): Unit = {

  }

  def parser(): Unit ={

  }

  def getExportSource(): SourceFunction.SourceContext[String] => Unit = {
    sc: SourceContext[String] => {
      while(true) {
        val downloadLink = config.getString("gdelt.last_15_minus_reference")
        val exportLink = getExportLink(downloadLink)
//        val exportZip = getExportZipFile(exportLink)
//        val exportData = parser(exportZip)
//        exportData
        sc.collect(exportLink)
        Thread.sleep(config.getInt("gdelt.interval"))
      }
    }
  }
}
