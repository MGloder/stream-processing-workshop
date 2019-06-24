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

  def getAllExportLink(downloadLink: String): List[String] = {
    Source
      .fromURL(downloadLink)
      .mkString
      .split("\\\n")
      .toList
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
    new File(file).delete()
    path + "/" + filename
  }

  def getExportSource(tempFolder: String = "/tmp"): SourceFunction.SourceContext[String] => Unit = {
    sc: SourceContext[String] => {
      while(true) {
        val downloadLink = config.getString("gdelt.last_15_minus_reference")
        val exportLink = getExportLink(downloadLink)
        val exportZip = getExportZipFile(exportLink)
        val exportData = extractExportZip(exportZip, tempFolder)
        sc.collect(exportData)
        Thread.sleep(config.getInt("gdelt.interval"))
      }
    }
  }

  def getAllExportSource(tempFolder: String = "/tmp"): SourceFunction.SourceContext[String] => Unit = {
    sc: SourceContext[String] => {
      val downloadLink = config.getString("gdelt.all_links")
      val exportLink = getAllExportLink(downloadLink)
      exportLink
        .map(s => s.split("\\s"))
        .filter(s => s.size == 3)
        .filter(s => s.contains("export"))
        .map(
          zipDownloadLink => getExportZipFile(zipDownloadLink(2)))
        .map(
          zipFilePath => extractExportZip(zipFilePath, tempFolder)
        )
        .map(sc.collect(_))
    }
  }
}
