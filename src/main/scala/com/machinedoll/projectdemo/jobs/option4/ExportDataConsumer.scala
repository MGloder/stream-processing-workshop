package com.machinedoll.projectdemo.jobs.option4

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.commons.logging.LogFactory
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import org.apache.flink.api.scala._


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
      .print()

    env.execute("Execute Export Data Consumer")
  }
}
