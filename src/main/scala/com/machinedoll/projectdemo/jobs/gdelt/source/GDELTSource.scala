package com.machinedoll.projectdemo.jobs.gdelt.source

import java.io.File
import java.net.URL
import java.util.Calendar
import java.util.zip.ZipFile

import com.machinedoll.projectdemo.schema.{Export, GDELTReferenceLink}
import com.typesafe.config.Config
import org.apache.commons.io.{FileUtils, FilenameUtils, IOUtils}
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

  def getExportZipFile(exportLink: String): String = {
    val url = new URL(exportLink)
    val file = new File(FilenameUtils.getName(url.getPath))
    FileUtils.copyURLToFile(url, new File(s"/tmp/${file}"))
    "/tmp/"+FilenameUtils.getName(url.getPath)
  }

  def extractExportZip(file: String, path: String): String ={
    import java.io.FileOutputStream
    val zipFile = new ZipFile(file)
    var filename: String = ""
    try {
      val entries = zipFile.entries
      while (entries.hasMoreElements) {
        val entry = entries.nextElement
        val entryDestination = new File(path, entry.getName)
        filename = entry.getName
        if (entry.isDirectory) entryDestination.mkdirs()
        else {
          entryDestination.getParentFile.mkdirs
          val in = zipFile.getInputStream(entry)
          val out = new FileOutputStream(entryDestination)
          IOUtils.copy(in, out)
          IOUtils.closeQuietly(in)
          out.close()
        }
      }
    } finally zipFile.close()
    path + "/" + filename
  }

  def getExportSource(): SourceFunction.SourceContext[String] => Unit = {
    sc: SourceContext[String] => {
      while(true) {
        val downloadLink = config.getString("gdelt.last_15_minus_reference")
        val exportLink = getExportLink(downloadLink)
        val exportZip = getExportZipFile(exportLink)
        val exportData = extractExportZip(exportZip, "/tmp")
        sc.collect(exportData)
        Thread.sleep(config.getInt("gdelt.interval"))
      }
    }
  }
}
