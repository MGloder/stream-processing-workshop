package com.machinedoll.projectdemo.source

import com.machinedoll.projectdemo.conf.pravega.Constants
import com.machinedoll.projectdemo.schema.GDELTReferenceLink
import com.machinedoll.projectdemo.utils.Pravega
import com.typesafe.config.Config
import io.pravega.connectors.flink.serialization.PravegaSerialization
import io.pravega.connectors.flink.{FlinkPravegaReader, PravegaConfig}
import org.apache.flink.api.java.utils.ParameterTool

case class PravegaSource(conf: Config, params: ParameterTool) extends StreamCustomSource[GDELTReferenceLink]{
  override def getCustomSource(): FlinkPravegaReader[GDELTReferenceLink] = {
    val pravegeConfig = PravegaConfig
      .fromParams(params)
      .withDefaultScope(Constants.DEFAULT_SCOPE)

    val stream = Pravega.createStream(
      pravegeConfig,
      params.get(Constants.STREAM_PARAM, Constants.DEFAULT_STREAM))

    FlinkPravegaReader
      .builder[GDELTReferenceLink]
      .withPravegaConfig(pravegeConfig)
      .forStream(stream)
      .withDeserializationSchema(PravegaSerialization.deserializationFor(classOf[GDELTReferenceLink]))
      .build
  }
}
