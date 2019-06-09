package com.machinedoll.projectdemo.sink
import java.net.URI

import com.machinedoll.projectdemo.conf.pravega.Constants
import com.machinedoll.projectdemo.schema.GDELTReferenceLink
import com.machinedoll.projectdemo.utils.Pravega
import com.typesafe.config.Config
import io.pravega.client.stream.{ScalingPolicy, StreamConfiguration}
import io.pravega.connectors.flink.serialization.PravegaSerialization
import io.pravega.connectors.flink.{FlinkPravegaWriter, PravegaConfig, PravegaEventRouter, PravegaWriterMode}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.java.utils.ParameterTool

class GDELTReferenceLinkPravegaSink(config: Config, params: ParameterTool) extends
  StreamCustomSink[GDELTReferenceLink](params: ParameterTool){
  override def getCustomSink(): FlinkPravegaWriter[GDELTReferenceLink] = {
    FlinkPravegaWriter
      .builder[GDELTReferenceLink]
      .withPravegaConfig(pravegaConfig)
      .forStream(stream)
      .withEventRouter(GDETLReferenceLinkRouter)
//      .withTxnLeaseRenewalPeriod(txnLeaseRenewalPeriod)
//      .withWriterMode(if (exactlyOnce) PravegaWriterMode.EXACTLY_ONCE else PravegaWriterMode.ATLEAST_ONCE)
      .withSerializationSchema(PravegaSerialization.serializationFor(classOf[GDELTReferenceLink]))
      .build
  }
}

object GDETLReferenceLinkRouter extends PravegaEventRouter[GDELTReferenceLink] {
  override def getRoutingKey(event: GDELTReferenceLink): String = "fixed_key"
}


