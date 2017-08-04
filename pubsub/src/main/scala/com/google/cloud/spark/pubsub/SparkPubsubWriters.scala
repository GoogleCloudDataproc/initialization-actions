package com.google.cloud.spark.pubsub

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

/**
  * Allows for writing RDD records to Pub/Sub
  *
  * To complete the pipeline, this will allows
  * users to take records they've processed in Spark
  * and write them back to a Pub/Sub topic
  */

class DStreamPubsubWriter(dStream: DStream[String]) extends Serializable {
  def writeToPubsub(client: PubsubClient) = {
    dStream.foreachRDD(rdd => {
      client.publishMessages(rdd.collect())
    })
  }
}


object DStreamPubsubWriter {
  implicit def pushDStreamPubsubWriter(dStream: DStream[String]) =
    new DStreamPubsubWriter(dStream)
}


class RDDPubsubWriter(rdd: RDD[String]) extends Serializable {
  def writeToPubsub(client: PubsubClient) = {
    client.publishMessages(rdd.collect())
  }
}


object RDDPubsubWriter {
  implicit def pushRDDPubsubWriter(rdd: RDD[String]) =
    new RDDPubsubWriter(rdd)
}