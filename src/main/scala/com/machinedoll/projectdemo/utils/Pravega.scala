package com.machinedoll.projectdemo.utils

import io.pravega.client.admin.StreamManager
import io.pravega.client.stream.{Stream, StreamConfiguration}
import io.pravega.connectors.flink.PravegaConfig

object Pravega {
//  def createStream(pravegaConfig: PravegaConfig, streamName: String, streamConfig: StreamConfiguration): Stream = {
//    val stream = pravegaConfig.resolve(streamName)
//    val streamManager: StreamManager = StreamManager.create(pravegaConfig.getClientConfig)
//    streamManager.createScope(stream.getScope)
//    streamManager.createStream(stream.getScope, stream.getStreamName, streamConfig)
//    stream
//  }
//
//  def createStream(pravegaConfig: PravegaConfig, streamName: String): Stream =
//    createStream(pravegaConfig, streamName, StreamConfiguration.builder.build)
}
