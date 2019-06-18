package com.machinedoll.projectdemo.sink
import com.machinedoll.projectdemo.schema.{Export, GDELTReferenceLink}
import com.typesafe.config.Config
import io.pravega.connectors.flink.serialization.PravegaSerialization
import io.pravega.connectors.flink.{FlinkPravegaWriter, PravegaEventRouter}
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

  def getExportSink(): FlinkPravegaWriter[Export] = {
    FlinkPravegaWriter
      .builder[Export]
      .withPravegaConfig(pravegaConfig)
      .forStream(stream)
      .withEventRouter(GDETLExportRouter)
      .withSerializationSchema(PravegaSerialization.serializationFor(classOf[Export]))
      .build
  }
}

object GDETLReferenceLinkRouter extends PravegaEventRouter[GDELTReferenceLink] {
  override def getRoutingKey(event: GDELTReferenceLink): String = "fixed_key"
}

object GDETLExportRouter extends PravegaEventRouter[Export] {
  override def getRoutingKey(event: Export): String = "event_export_data"
}


