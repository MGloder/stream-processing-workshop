package com.machinedoll.projectdemo.sink

import com.machinedoll.projectdemo.schema.{FileInfo, GDELTReferenceLink}
import com.typesafe.config.Config
import io.pravega.connectors.flink.{FlinkPravegaWriter, PravegaEventRouter}
import io.pravega.connectors.flink.serialization.PravegaSerialization
import org.apache.flink.api.java.utils.ParameterTool

case class GDELTZipFilePravegaSink(conf: Config, params: ParameterTool) extends
  StreamCustomSink[FileInfo](params: ParameterTool){
  override def getCustomSink(): FlinkPravegaWriter[FileInfo] = {
    FlinkPravegaWriter
      .builder[FileInfo]
      .withPravegaConfig(pravegaConfig)
      .forStream(stream)
      .withEventRouter(FileInfoRouter)
      .withSerializationSchema(PravegaSerialization.serializationFor(classOf[FileInfo]))
      .build
  }
}

object FileInfoRouter extends PravegaEventRouter[FileInfo] {
  override def getRoutingKey(event: FileInfo): String = "file_info"
}