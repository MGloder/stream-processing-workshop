package com.machinedoll.projectdemo.source

import io.pravega.connectors.flink.FlinkPravegaReader

trait StreamCustomSource[T] {
  def getCustomSource(): FlinkPravegaReader[T]
}
