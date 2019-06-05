package com.machinedoll.wonderland.source

import java.util.Calendar

import com.machinedoll.wonderland.entity.GDELTDownloadReference

import com.typesafe.config.Config
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

import scala.io.Source

class GDELTLinkSource(config: Config) extends CustomBatchSource[GDELTDownloadReference] with Serializable {
  def getLatestReferenceLink(downloadLink: String)  =
    Source
      .fromURL(downloadLink)
      .mkString
      .split("\\\n")
      .toList
      .map(instance => {
        instance.split("\\s").toList
      })
      .map(ref => GDELTDownloadReference(ref(0).toDouble, ref(1), ref(2), Calendar.getInstance.getTimeInMillis.toString))

  override def getSource: SourceFunction.SourceContext[GDELTDownloadReference] => Unit = {
    sc: SourceContext[GDELTDownloadReference] => {
      while (true) {
        val downloadLink = config.getString("gdelt.last_15_minus_reference")
        val dgeltDownloadReferenceList = getLatestReferenceLink(downloadLink)
        dgeltDownloadReferenceList.map(
          refEntity => sc.collect(refEntity)
        )
        Thread.sleep(config.getInt("gdelt.interval"))
      }
    }
  }

}
