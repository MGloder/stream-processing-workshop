package com.machinedoll.projectdemo.source

import com.machinedoll.projectdemo.conf.pravega.Constants
import com.machinedoll.projectdemo.schema.{FileInfo, GDELTReferenceLink}
import com.machinedoll.projectdemo.utils.Pravega
import com.typesafe.config.Config
import io.pravega.connectors.flink.serialization.PravegaSerialization
import io.pravega.connectors.flink.{FlinkPravegaReader, PravegaConfig}
import org.apache.flink.api.java.utils.ParameterTool

case class ReferenceLinkPravegaSource(conf: Config, params: ParameterTool) extends
  StreamCustomSource[GDELTReferenceLink](conf: Config, params: ParameterTool){
  override def getCustomSource(): FlinkPravegaReader[GDELTReferenceLink] = {
    FlinkPravegaReader
      .builder[GDELTReferenceLink]
      .withPravegaConfig(pravegeConfig)
      .forStream(stream)
      .withDeserializationSchema(PravegaSerialization.deserializationFor(classOf[GDELTReferenceLink]))
      .build
  }
}

case class FileInfoPravegaSource(conf: Config, params: ParameterTool) extends
  StreamCustomSource[FileInfo](conf: Config, params: ParameterTool){
  override def getCustomSource(): FlinkPravegaReader[FileInfo] = {
    FlinkPravegaReader
      .builder[FileInfo]
      .withPravegaConfig(pravegeConfig)
      .forStream(stream)
      .withDeserializationSchema(PravegaSerialization.deserializationFor(classOf[FileInfo]))
      .build
  }
}
