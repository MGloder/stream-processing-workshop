package com.machinedoll.projectdemo.jobs.option4

import java.text.SimpleDateFormat
import java.util
import java.util.Properties

import com.machinedoll.projectdemo.jobs.gdelt.source.EventSplitter
import com.machinedoll.projectdemo.schema.Export
import com.typesafe.config.ConfigFactory
import org.apache.commons.logging.LogFactory
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests


object ExportDataConsumer {
  def LOG = LogFactory.getLog(ExportDataConsumer.getClass)
  def main(args: Array[String]): Unit = {
    LOG.info("Starting Loading export data to elasticsearch...")
    val conf = ConfigFactory.load
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0L))

    env.enableCheckpointing(450000, CheckpointingMode.EXACTLY_ONCE)

    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    // only required for Kafka 0.8
    properties.setProperty("zookeeper.connect", "localhost:2181")
    properties.setProperty("group.id", "test")
    val stream = env
      .addSource(new FlinkKafkaConsumer[String]("gdelt-topic-example", new SimpleStringSchema(), properties))
      .flatMap(EventSplitter())

    val esSinkConfig: ElasticsearchSinkFunction[Export] = new ElasticsearchSinkFunction[Export] {
      val dateFormat = new SimpleDateFormat("yyyymmdd")
      val outputFormat = new SimpleDateFormat("yyyy-mm-dd HH:mm:ss")
      def updateJsonObject(t: Export) = {
        val json = new util.HashMap[String, String]()
        json.put("GLOBALEVENTID", t.GLOBALEVENTID.get.toString)
        json.put("SQLDATE", outputFormat.format(dateFormat.parse(t.SQLDATE.getOrElse(0).toString)))
        json.put("MonthYear", t.MonthYear.getOrElse(0).toString)
        json.put("Year", t.Year.getOrElse(0).toString)
        json.put("FractionDate", t.FractionDate.getOrElse(0).toString)
        json.put("Actor1Code", t.Actor1Code.getOrElse(0).toString)
        json.put("Actor1Name", t.Actor1Name.getOrElse(0).toString)
        json.put("Actor1CountryCode", t.Actor1CountryCode.getOrElse(0).toString)
        json.put("Actor1KnownGroupCode", t.Actor1KnownGroupCode.getOrElse(0).toString)
        json.put("Actor1EthnicCode", t.Actor1EthnicCode.getOrElse(0).toString)
        json.put("Actor1Religion1Code", t.Actor1Religion1Code.getOrElse(0).toString)
        json.put("Actor1Religion2Code", t.Actor1Religion2Code.getOrElse(0).toString)
        json.put("Actor1Type1Code", t.Actor1Type1Code.getOrElse(0).toString)
        json.put("Actor1Type2Code", t.Actor1Type2Code.getOrElse(0).toString)
        json.put("Actor1Type3Code", t.Actor1Type3Code.getOrElse(0).toString)
        json.put("Actor2Code", t.Actor2Code.getOrElse(0).toString)
        json.put("Actor2Name", t.Actor2Name.getOrElse(0).toString)
        json.put("Actor2CountryCode", t.Actor2CountryCode.getOrElse(0).toString)
        json.put("Actor2KnownGroupCode", t.Actor2KnownGroupCode.getOrElse(0).toString)
        json.put("Actor2EthnicCode", t.Actor2EthnicCode.getOrElse(0).toString)
        json.put("Actor2Religion1Code", t.Actor2Religion1Code.getOrElse(0).toString)
        json.put("Actor2Religion2Code", t.Actor2Religion2Code.getOrElse(0).toString)
        json.put("Actor2Type1Code", t.Actor2Type1Code.getOrElse(0).toString)
        json.put("Actor2Type2Code", t.Actor2Type2Code.getOrElse(0).toString)
        json.put("Actor2Type3Code", t.Actor2Type3Code.getOrElse(0).toString)
        json.put("IsRootEvent", t.IsRootEvent.getOrElse(0).toString)
        json.put("EventCode", t.EventCode.getOrElse(0).toString)
        json.put("EventBaseCode", t.EventBaseCode.getOrElse(0).toString)
        json.put("EventRootCode", t.EventRootCode.getOrElse(0).toString)
        json.put("QuadClass", t.QuadClass.getOrElse(0).toString)
        json.put("GoldsteinScale", t.GoldsteinScale.getOrElse(0).toString)
        json.put("NumMentions", t.NumMentions.getOrElse(0).toString)
        json.put("NumSources", t.NumSources.getOrElse(0).toString)
        json.put("NumArticles", t.NumArticles.getOrElse(0).toString)
        json.put("AvgTone", t.AvgTone.getOrElse(0).toString)
        json.put("Actor1Geo_Type", t.Actor1Geo_Type.getOrElse(0).toString)
        json.put("Actor1Geo_FullName", t.Actor1Geo_FullName.getOrElse(0).toString)
        json.put("Actor1Geo_CountryCode", t.Actor1Geo_CountryCode.getOrElse(0).toString)
        json.put("Actor1Geo_ADM1Code", t.Actor1Geo_ADM1Code.getOrElse(0).toString)
        json.put("Actor1Geo_ADM2Code", t.Actor1Geo_ADM2Code.getOrElse(0).toString)
        json.put("Actor1Geo_Lat", t.Actor1Geo_Lat.getOrElse(0).toString)
        json.put("Actor1Geo_Long", t.Actor1Geo_Long.getOrElse(0).toString)
        json.put("Actor1Geo_FeatureID", t.Actor1Geo_FeatureID.getOrElse(0).toString)
        json.put("Actor2Geo_Type", t.Actor2Geo_Type.getOrElse(0).toString)
        json.put("Actor2Geo_FullName", t.Actor2Geo_FullName.getOrElse(0).toString)
        json.put("Actor2Geo_CountryCode", t.Actor2Geo_CountryCode.getOrElse(0).toString)
        json.put("Actor2Geo_ADM1Code", t.Actor2Geo_ADM1Code.getOrElse(0).toString)
        json.put("Actor2Geo_ADM2Code", t.Actor2Geo_ADM2Code.getOrElse(0).toString)
        json.put("Actor2Geo_Long", t.Actor2Geo_Long.getOrElse(0).toString)
        json.put("Actor2Geo_FeatureID", t.Actor2Geo_FeatureID.getOrElse(0).toString)
        json.put("ActionGeo_Type", t.ActionGeo_Type.getOrElse(0).toString)
        json.put("ActionGeo_FullName", t.ActionGeo_FullName.getOrElse(0).toString)
        json.put("ActionGeo_CountryCode", t.ActionGeo_CountryCode.getOrElse(0).toString)
        json.put("ActionGeo_ADM1Code", t.ActionGeo_ADM1Code.getOrElse(0).toString)
        json.put("ActionGeo_ADM2Code", t.ActionGeo_ADM2Code.getOrElse(0).toString)
        json.put("ActionGeo_Lat", t.ActionGeo_Lat.getOrElse(0).toString)
        json.put("ActionGeo_Long", t.ActionGeo_Long.getOrElse(0).toString)
        json.put("ActionGeo_FeatureID", t.ActionGeo_FeatureID.getOrElse(0).toString)
        json.put("DATEADDED", t.DATEADDED.getOrElse(0).toString)
        json.put("SOURCEURL", t.SOURCEURL.getOrElse(0).toString)
        json
      }

      override def process(t: Export, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        val reuslt = updateJsonObject(t)
        val request = Requests.indexRequest()
          .index("gdelt")
          .`type`("export")
          .source(reuslt)
        requestIndexer.add(request)
      }
//
//      override def invoke(txn: TXN, in: IN, context: SinkFunction.Context[_]): Unit = ???
//
//      override def beginTransaction(): TXN = ???
//
//      override def preCommit(txn: TXN): Unit = ???
//
//      override def commit(txn: TXN): Unit = ???
//
//      override def abort(txn: TXN): Unit = ???
    }

    val httpHosts = new util.ArrayList[HttpHost]
    httpHosts.add(new HttpHost("127.0.0.1", 9200, "http"))

    val esSinkBuilder = new ElasticsearchSink.Builder[Export](
      httpHosts,
      esSinkConfig
    )

    esSinkBuilder.setBulkFlushMaxActions(1)

    stream.addSink(esSinkBuilder.build())

    env.execute("Execute Export Data Consumer")
  }

}
