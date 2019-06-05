package com.machinedoll.projectdemo.sink

import com.typesafe.config.Config
import io.pravega.connectors.flink.FlinkPravegaWriter
import org.apache.flink.api.java.utils.ParameterTool

trait StreamCustomSink[T] {
  def getCustomSink(): FlinkPravegaWriter[T]
}
