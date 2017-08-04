package com.google.cloud.spark.pubsub

import com.google.cloud.pubsub.spi.v1.Subscriber
import com.google.pubsub.v1.PubsubMessage
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Returns a DStream from a custom receiver
  *
  * As per the more canonical implementations
  * of custom receiver, this class acts as a
  * factory for creating DStreams of records
  */
object PubsubUtils {
  def createStream(ssc: StreamingContext, projectId: String,
                   topicId: String,
                   subscriptionId: String): ReceiverInputDStream[PubsubMessage] = {
    ssc.receiverStream(new PubsubReceiver(
      PubsubStreamOptions(projectId, topicId, subscriptionId),
      (subscription, receiver) => Subscriber.defaultBuilder(subscription, receiver).build
    ))
  }
}