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

import com.google.cloud.pubsub.v1.Subscriber
import com.google.pubsub.v1.PubsubMessage
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

/**
 * Returns a DStream from a custom receiver
 *
 * As per the more canonical implementations
 * of custom receiver, this class acts as a
 * factory for creating DStreams of records
 */
object PubsubUtils {
  def createStream(
      ssc: StreamingContext,
      projectId: String,
      subscriptionId: String): DStream[PubsubMessage] = {
    createStream(ssc, projectId, subscriptionId, ssc.sparkContext.defaultParallelism)
  }

  def createStream(
      ssc: StreamingContext,
      projectId: String,
      subscriptionId: String,
      parallelism: Int): DStream[PubsubMessage] = {
    ssc.union(
      1 to parallelism map (_ =>
        ssc.receiverStream(new PubsubReceiver(
          PubsubStreamOptions(projectId, subscriptionId),
          (subscription, receiver) => Subscriber.defaultBuilder(subscription, receiver).build))))
  }
}
