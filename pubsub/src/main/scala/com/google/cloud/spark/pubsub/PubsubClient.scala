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

import com.google.api.core.ApiFuture
import com.google.cloud.pubsub.v1.{Publisher, SubscriptionAdminClient, TopicAdminClient}
import com.google.cloud.spark.pubsub.stackdriver.PubsubMetrics
import com.google.protobuf.{ByteString, Timestamp}
import com.google.pubsub.v1.{PubsubMessage, PushConfig, SubscriptionName, TopicName}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer

/** Handles interactions with PubSub in Scala
 *
 * For our integration tests, were going to want some automated way of
 * creating a topic on PubSub, publishing a sizable number of records, and
 * (once were done streaming) tearing it down. This will probably expose
 * some method to allow us to upload files to demonstrate versatility of
 * data processing by trying streaming on various types of data. We may also
 * want to consider implementing a static subscriber method as described
 * in the PubSub to verify output.
 */
class PubsubClient(
  projectId: String,
  topicId: String,
  stackDriverPollingInterval: Int = 1000) extends Serializable {

  @transient private lazy val topicAdminClient = TopicAdminClient.create
  @transient private lazy val subscriptionAdminClient = SubscriptionAdminClient.create
  private val log = LoggerFactory.getLogger(getClass)

  def createTopic(): Unit = {
    val topic = TopicName.create(projectId, topicId)
    topicAdminClient.createTopic(topic)
  }

  def deleteTopic(): Unit = {
    val topic = TopicName.create(projectId, topicId)
    topicAdminClient.deleteTopic(topic)
  }

  def publishMessages(messages: TraversableOnce[String]): Seq[ApiFuture[String]] = {
    /*
    Using same structure as recommended by Google PubSub Java Docs
    https://cloud.google.com/pubsub/docs/publisher#pubsub-publish-java
     */
    val topic = TopicName.create(projectId, topicId)
    var publisher: Publisher = null
    val messageIdFutures: ListBuffer[ApiFuture[String]] = ListBuffer.empty[ApiFuture[String]]

    try {
      publisher = Publisher.defaultBuilder(topic).build
      messages.foreach { message =>
        val millis = System.currentTimeMillis

        // Construct PubSub message from string
        val pMessage = PubsubMessage.newBuilder
          .setData(ByteString.copyFromUtf8(message))
          .setPublishTime(
            Timestamp.newBuilder
              .setSeconds(millis / 1000)
              .setNanos((millis.toInt % 1000) * 1000000)
              .build)
          .build

        // Once published, returns a server-assigned message id (unique within the topic)
        messageIdFutures += publisher.publish(pMessage)
      }
    } finally {
      // wait on any pending publish requests.
      if (publisher != null) {
        publisher.shutdown()
      }
    }
    messageIdFutures
  }

  def publishMessagesAndWait(messages: TraversableOnce[String]): Unit = {
    publishMessages(messages).foreach(_.get)
  }

  def createSubscription(subscriptionId: String): SubscriptionName = {
    val topic = TopicName.create(projectId, topicId)
    val subscription = SubscriptionName.create(projectId, subscriptionId)
    subscriptionAdminClient.createSubscription(
      subscription, topic, PushConfig.getDefaultInstance, 0)
    subscription
  }

  def blockUntilNoUndeliveredMessages(
    subscriptionId: String,
    metricReceiver: PubsubMetrics): Unit = {
    var topicResponse = metricReceiver
      .getMetric("num_undelivered_messages", subscriptionId)

    while (!topicResponse.contains(0)) {
      Thread.sleep(stackDriverPollingInterval)
      topicResponse = metricReceiver.getMetric("num_undelivered_messages", subscriptionId)
    }
  }
}
