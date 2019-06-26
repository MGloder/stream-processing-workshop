package com.machinedoll.projectdemo.jobs.flink_kafka_example.table

import java.util.Properties

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors.{Csv, Kafka, Schema}

object TableAPISpike {


  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

//    val typeInfo = {Types.STRING}
//    val tableSchema: TableSchema = TableSchema.builder().field("aa", Types.STRING).build()
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    // only required for Kafka 0.8
    properties.setProperty("zookeeper.connect", "localhost:2181")
    properties.setProperty("group.id", "test")

    val topic = "gdelt-topic-example"
//    var schema: TableSchema = new TableSchema()

    val tableEnv = StreamTableEnvironment.create(env)
//    tableEnv.connect(new Kafka().version("2.2.0")
//        .topic(topic)
//        .property("bootstrap.servers", "localhost:9092")
//        .property("zookeeper.connect", "localhost:2181")
//        .property("group.id", "test")
//        .startFromLatest()
//    ).withFormat()
//      .withSchema(new Schema()
//      .schema(schema)
//        .proctime()
//    )
//    val tableSource = new KafkaTableSource("gdelt-topic-example", properties, SimpleStringSchema, typeInfo)
  }
}
