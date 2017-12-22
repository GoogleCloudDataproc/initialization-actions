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

import com.google.api.core.ApiService
import com.google.api.core.ApiService.State
import com.google.cloud.pubsub.v1.{AckReplyConsumer, MessageReceiver, Subscriber}
import com.google.common.util.concurrent.MoreExecutors
import com.google.pubsub.v1.{PubsubMessage, SubscriptionName}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.slf4j.LoggerFactory

/**
 * Manages custom Spark receiver logic
 *
 * This is where the bulk of the receiver logic would be implemented
 * specifically managing the creation of a client to Pub/Sub and then
 * storing those messages to Spark
 */
class PubsubReceiver(options: PubsubStreamOptions,
                     buildSubscriber: (SubscriptionName, MessageReceiver) => Subscriber)
  extends Receiver[PubsubMessage](StorageLevel.MEMORY_AND_DISK_2) {

  private val log = LoggerFactory.getLogger(getClass)
  var subscriber: Subscriber = null

  // For Spark worker serialization reason this work
  // needs to be done in an init method
  def init() {
    // (TODO) Investigate thread safety
    if (subscriber != null) {return}

    val subscription: SubscriptionName = SubscriptionName
    .newBuilder
    .setProject(options.projectId)
    .setSubscription(options.subscriptionId)
    .build

    // Until stopped or connection broken continue reading
    val receiver = new PubsubMessageReceiver(this)
    subscriber = buildSubscriber(subscription, receiver)

    subscriber.addListener(
      new ApiService.Listener {
        override def failed(from: State, failure: Throwable) {
          log.error(failure.getMessage)
          throw failure
        }
      },
      MoreExecutors.directExecutor()
    )
  }

  def onStart() {
    init()
    subscriber.startAsync.awaitRunning()
  }

  def onStop() {
    // (TODO) awaitHealthy
    if (subscriber == null) {
      // Already stopped, no operation
      return
    }
    subscriber.stopAsync().awaitTerminated()
    subscriber = null
  }
}


class PubsubMessageReceiver(pubsubReceiver: PubsubReceiver)
  extends MessageReceiver {

  private val log = LoggerFactory.getLogger(getClass)

  override def receiveMessage(
    pubsubMessage: PubsubMessage,
    ackReplyConsumer: AckReplyConsumer): Unit = {
    pubsubReceiver.store(pubsubMessage) // (TODO) Store as batch for reliable receiver
    ackReplyConsumer.ack()
  }
}
