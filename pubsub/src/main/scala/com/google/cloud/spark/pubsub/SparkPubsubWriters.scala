/*
 * Copyright (c) 2017 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

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
  def writeToPubsub(clientProvider: () => PubsubClient): Unit = {
    dStream.foreachRDD(rdd => {
      new RDDPubsubWriter(rdd).writeToPubsub(clientProvider)
    })
  }

  def writeToPubsub(projectId: String, topicId: String): Unit = {
    writeToPubsub(() => new PubsubClient(projectId, topicId))
  }
}


object DStreamPubsubWriter {
  implicit def pushDStreamPubsubWriter(dStream: DStream[String]): DStreamPubsubWriter =
    new DStreamPubsubWriter(dStream)
}


class RDDPubsubWriter(rdd: RDD[String]) extends Serializable {
  def writeToPubsub(clientProvider: () => PubsubClient): Unit = {
    rdd.foreachPartition(clientProvider().publishMessagesAndWait)
  }

  def writeToPubsub(projectId: String, topicId: String): Unit = {
    writeToPubsub(() => new PubsubClient(projectId, topicId))
  }
}


object RDDPubsubWriter {
  implicit def pushRDDPubsubWriter(rdd: RDD[String]): RDDPubsubWriter =
    new RDDPubsubWriter(rdd)
}
