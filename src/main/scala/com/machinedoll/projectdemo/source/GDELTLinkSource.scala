package com.machinedoll.projectdemo.source

import java.util.Calendar

import com.machinedoll.projectdemo.schema.GDELTReferenceLink
import com.typesafe.config.Config
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

import scala.io.Source

class GDELTLinkSource(config: Config) extends CustomBatchSource[GDELTReferenceLink] with Serializable {
  def getLatestReferenceLink(downloadLink: String)  =
    Source
      .fromURL(downloadLink)
      .mkString
      .split("\\\n")
      .toList
      .map(instance => {
        instance.split("\\s").toList
      })
      .map(ref => GDELTReferenceLink(ref(0).toDouble, ref(1), ref(2), Calendar.getInstance.getTimeInMillis.toString))

  override def getSource: SourceFunction.SourceContext[GDELTReferenceLink] => Unit = {
    sc: SourceContext[GDELTReferenceLink] => {
      while (true) {
        val downloadLink = config.getString("gdelt.last_15_minus_reference")
        val dgGDELTReferenceLinkList = getLatestReferenceLink(downloadLink)
        dgGDELTReferenceLinkList.map(
          refEntity => sc.collect(refEntity)
        )
        Thread.sleep(config.getInt("gdelt.interval"))
      }
    }
  }

}
