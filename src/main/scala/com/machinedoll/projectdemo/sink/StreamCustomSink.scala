package com.machinedoll.projectdemo.sink

import java.net.URI

import com.machinedoll.projectdemo.conf.pravega.Constants
import com.machinedoll.projectdemo.utils.Pravega
import com.typesafe.config.Config
import io.pravega.client.stream.{ScalingPolicy, StreamConfiguration}
import io.pravega.connectors.flink.{FlinkPravegaWriter, PravegaConfig}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.java.utils.ParameterTool

abstract class StreamCustomSink[T](params: ParameterTool) {
  protected val txnLeaseRenewalPeriod: Time = Time.milliseconds(30 * 1000)
  protected val exactlyOnce: Boolean = params.get("exactly_once", "true").toBoolean
  protected val pravegaConfig = PravegaConfig
    .fromParams(params)
    .withControllerURI(URI.create(params.get(Constants.DEFAUTL_URI_PARAM, Constants.DEFAULT_URI)))
    .withDefaultScope(params.get(Constants.SCOPE_PARAM, Constants.DEFAULT_SCOPE))

  protected val stream = Pravega.createStream(
    pravegaConfig,
    params.get(Constants.STREAM_PARAM, Constants.DEFAULT_STREAM),
    StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build())
  def getCustomSink(): FlinkPravegaWriter[T]
}
