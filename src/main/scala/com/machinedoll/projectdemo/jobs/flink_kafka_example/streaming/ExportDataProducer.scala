package com.machinedoll.projectdemo.jobs.flink_kafka_example.streaming

import com.machinedoll.projectdemo.jobs.gdelt.source.GDELTSource
import com.typesafe.config.ConfigFactory
import org.apache.commons.logging.LogFactory
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.api.scala._

object ExportDataProducer {
  def LOG = LogFactory.getLog(ExportDataProducer.getClass)

  def main(args: Array[String]): Unit = {
    LOG.info("Starting Download Export Data...")

    val conf = ConfigFactory.load

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.enableCheckpointing(450000, CheckpointingMode.EXACTLY_ONCE)

    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0L))

    val tempFolder = "/Users/xyan/Data/GDELT"
    val exportSource = new GDELTSource(conf).getExportSource(tempFolder)
    val kafkaSink = new FlinkKafkaProducer[String](
      "localhost:9092",
      "gdelt-topic-example",
      new SimpleStringSchema
    //      new KeyedSerializationSchema[Export] {
//        override def serializeKey(t: Export): Array[Byte] = ("\"" + t.GLOBALEVENTID.get.toString + "\"").getBytes()
//
//        override def serializeValue(t: Export): Array[Byte] = t.toString.getBytes()
//
//        override def getTargetTopic(t: Export): String = null
//      }

    )

    kafkaSink.setWriteTimestampToKafka(true)

    env
      .addSource(exportSource)
      .print()

    val format: TextInputFormat = new TextInputFormat(new Path(tempFolder))
    val text = env.readFile(format, tempFolder, FileProcessingMode.PROCESS_CONTINUOUSLY, conf.getLong("gdelt.dir.interval"))
    val eventExportData = text
    eventExportData.addSink(kafkaSink)
    env.execute("Download Export Data")
  }
}
