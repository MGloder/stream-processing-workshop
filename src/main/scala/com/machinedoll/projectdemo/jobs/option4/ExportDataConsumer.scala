package com.machinedoll.projectdemo.jobs.option4

import java.util
import java.util.Properties

import com.machinedoll.projectdemo.jobs.gdelt.source.EventSplitter
import com.machinedoll.projectdemo.schema.Export
import com.typesafe.config.ConfigFactory
import org.apache.commons.logging.LogFactory
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests


object ExportDataConsumer {
  def LOG = LogFactory.getLog(ExportDataConsumer.getClass)
  def main(args: Array[String]): Unit = {
    LOG.info("Starting Loading export data to elasticsearch...")
    val conf = ConfigFactory.load
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0L))

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    // only required for Kafka 0.8
    properties.setProperty("zookeeper.connect", "localhost:2181")
    properties.setProperty("group.id", "test")
    val stream = env
      .addSource(new FlinkKafkaConsumer[String]("gdelt-topic-example", new SimpleStringSchema(), properties))
      .flatMap(EventSplitter())

    val esSinkConfig: ElasticsearchSinkFunction[Export] = new ElasticsearchSinkFunction[Export] {
      override def process(t: Export, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        val json = new util.HashMap[String, String]()
//        json.put("ptimestamp", t.ptimestamp)
        ???
        val request = Requests.indexRequest()
          .index("gdelt")
          .`type`("export")
          .source(json)
        requestIndexer.add(request)
      }
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

//  def GDELTSource(conf: Config, query_url: String):SourceFunction.SourceContext[GDELTDownloadLinkReference] => Unit = {
//    (sc: SourceContext[GDELTDownloadLinkReference]) => {
//      while (true) {
//        val downloadLink = query_url match {
//          //          case "all" => conf.getString("gdelt.all_links")
//          case "latest" => conf.getString("gdelt.last_15_minus_reference")
//          case other => ""
//        }
//        val dgeltReferenceList = getLatestReferenceLink(downloadLink)
//        dgeltReferenceList.map(
//          downloadEntity => sc.collect(downloadEntity)
//        )
//        Thread.sleep(conf.getInt("gdelt.interval"))
//      }
//    }
//  }
//
//  def getLatestReferenceLink(link: String) =
//    Source
//      .fromURL(link)
//      .mkString
//      .split("\\\n")
//      .toList
//      .map(instance => {
//        instance.split("\\s").toList
//      })
//      .map(x => GDELTDownloadLinkReference(x(0).toDouble, x(1), x(2), Calendar.getInstance.getTimeInMillis.toString))

}
