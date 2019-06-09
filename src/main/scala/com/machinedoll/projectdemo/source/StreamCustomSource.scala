package com.machinedoll.projectdemo.source

import com.machinedoll.projectdemo.conf.pravega.Constants
import com.machinedoll.projectdemo.utils.Pravega
import com.typesafe.config.Config
import io.pravega.client.stream.{ScalingPolicy, StreamConfiguration}
import io.pravega.connectors.flink.{FlinkPravegaReader, PravegaConfig}
import org.apache.flink.api.java.utils.ParameterTool

abstract class StreamCustomSource[T](conf: Config, params: ParameterTool) {
  protected val pravegeConfig = PravegaConfig
    .fromParams(params)
    .withDefaultScope(Constants.DEFAULT_SCOPE)

  protected val stream = Pravega.createStream(
    pravegeConfig,
    Constants.DEFAULT_STREAM,
    StreamConfiguration.builder.scalingPolicy(ScalingPolicy.fixed(1)).build)

  def getCustomSource(): FlinkPravegaReader[T]
}
