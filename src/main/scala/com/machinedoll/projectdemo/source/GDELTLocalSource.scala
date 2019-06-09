package com.machinedoll.projectdemo.source

import com.machinedoll.projectdemo.schema.GDELTRawData
import com.typesafe.config.Config
import io.pravega.shaded.com.google.protobuf.SourceContext
import org.apache.flink.streaming.api.functions.source.SourceFunction

class GDELTLocalSource(conf: Config) extends CustomBatchSource [GDELTRawData] with Serializable {
//  override def getSource: SourceFunction.SourceContext[GDELTRawData] => Unit = {
//    sc: SourceContext[GDELTRawData] => {
//      while(true) {
//        val tmpFolder = "/tmp"
//        Thread.sleep(config.getInt("gdelt.interval"))
//      }
//    }
//  }
}
