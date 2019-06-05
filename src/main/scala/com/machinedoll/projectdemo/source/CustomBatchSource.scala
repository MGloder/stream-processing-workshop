package com.machinedoll.projectdemo.source

import org.apache.flink.streaming.api.functions.source.SourceFunction

trait CustomBatchSource[T] {
  def getSource: SourceFunction.SourceContext[T] => Unit = {
    ???
  }
}
