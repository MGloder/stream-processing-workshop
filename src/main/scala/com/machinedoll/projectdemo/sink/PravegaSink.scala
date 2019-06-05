package com.machinedoll.projectdemo.sink
import java.net.URI

import com.machinedoll.projectdemo.conf.pravega.{Constants, IntegerEvent}
import com.machinedoll.projectdemo.entity.GDELTDownloadReference
import com.machinedoll.projectdemo.utils.Pravega
import com.typesafe.config.Config
import io.pravega.connectors.flink.serialization.PravegaSerialization
import io.pravega.connectors.flink.{FlinkPravegaWriter, PravegaConfig, PravegaEventRouter, PravegaWriterMode}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.java.utils.ParameterTool

class PravegaSink(config: Config, params: ParameterTool) extends StreamCustomSink[GDELTDownloadReference] {
  private val txnLeaseRenewalPeriod: Time = Time.milliseconds(30 * 1000)

  override def getCustomSink(): FlinkPravegaWriter[GDELTDownloadReference] = {
    val exactlyOnce: Boolean = params.get("exactly_once", "true").toBoolean
    val pravegaConfig = PravegaConfig
      .fromParams(params)
      .withControllerURI(URI.create(params.get(Constants.DEFAUTL_URI_PARAM, Constants.DEFAULT_URI)))
      .withDefaultScope(params.get(Constants.SCOPE_PARAM, Constants.DEFAULT_SCOPE))

    val stream = Pravega.createStream(pravegaConfig, params.get(Constants.STREAM_PARAM, Constants.DEFAULT_STREAM))

    FlinkPravegaWriter
      .builder[GDELTDownloadReference]
      .withPravegaConfig(pravegaConfig)
      .forStream(stream)
      .withEventRouter(GDETLReferenceLinkRouter)
      .withTxnLeaseRenewalPeriod(txnLeaseRenewalPeriod)
      .withWriterMode(if(exactlyOnce)  PravegaWriterMode.EXACTLY_ONCE else PravegaWriterMode.ATLEAST_ONCE)
      .withSerializationSchema(PravegaSerialization.serializationFor(classOf[GDELTDownloadReference]))
      .build
  }
}

object GDETLReferenceLinkRouter extends PravegaEventRouter[GDELTDownloadReference] {
  override def getRoutingKey(event: GDELTDownloadReference): String = "fixed_key"
}


