package com.google.cloud.spark.pubsub

import com.google.api.core.{ApiFuture, ApiService}
import com.google.api.gax.grpc.ApiException
import com.google.cloud.monitoring.spi.v3.MetricServiceClient
import com.google.cloud.pubsub.spi.v1._
import com.google.cloud.spark.pubsub.stackdriver.PubsubMetrics
import com.google.common.util.concurrent.MoreExecutors
import com.google.protobuf.{ByteString, Timestamp}
import com.google.pubsub.v1.{PubsubMessage, PushConfig, SubscriptionName, TopicName}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer

/**
  * Handles interactions with PubSub in Scala
  *
  * For our integration tests, we’re going to want some automated way of
  * creating a topic on PubSub, publishing a sizable number of records, and
  * (once we’re done streaming) tearing it down. This will probably expose
  * some method to allow us to upload files to demonstrate versatility of
  * data processing by trying streaming on various types of data. We may also
  * want to consider implementing a static subscriber method as described
  * in the PubSub to verify output.
  */

class PubsubClient(projectId: String, topicId: String) {
  val topic = TopicName.create(projectId, topicId)
  val topicAdminClient = TopicAdminClient.create
  val subscriptionAdminClient = SubscriptionAdminClient.create

  val STACKDRIVER_POLLING_INTERVAL = 1000

  private val log = LoggerFactory.getLogger(getClass)

  def createTopic() : Unit = {
    topicAdminClient.createTopic(topic)
  }

  def deleteTopic() : Unit = {
    topicAdminClient.deleteTopic(topic)
  }

  def publishMessages(messages: Array[String]) : Seq[ApiFuture[String]] = {
    /*
    Using same structure as recommended by Google PubSub Java Docs
    https://cloud.google.com/pubsub/docs/publisher#pubsub-publish-java
     */
    var publisher: Publisher = null
    val messageIdFutures: ListBuffer[ApiFuture[String]] = ListBuffer.empty[ApiFuture[String]]

    try {
      // Create a publisher instance with default settings bound to the topic
      publisher = Publisher.defaultBuilder(topic).build

      messages.foreach(message => {
        val millis = System.currentTimeMillis

        // Construct PubSub message from string
        val pmessage = PubsubMessage
          .newBuilder
          .setData(ByteString.copyFromUtf8(message))
          .setPublishTime(
            Timestamp
              .newBuilder
              .setSeconds(millis / 1000)
              .setNanos(((millis % 1000) * 1000000).toInt)
              .build
          ).build

        // Once published, returns a server-assigned message id (unique within the topic)
        val messageIdFuture = publisher.publish(pmessage)
        messageIdFutures += messageIdFuture
      })
    } finally {
      // wait on any pending publish requests.
      if (publisher != null) {
        publisher.shutdown()
      }
    }
    messageIdFutures
  }

  def createSubscription(subscriptionId: String) : SubscriptionName = {
    val subscription = SubscriptionName.create(projectId, subscriptionId)
    subscriptionAdminClient.createSubscription(subscription, topic, PushConfig.getDefaultInstance, 0)

    subscription
  }

  def getMessagesFromTopic(subscriptionId: String) : ListBuffer[PubsubMessage] = {
    val messageStore: ListBuffer[PubsubMessage] = ListBuffer.empty[PubsubMessage]

    val subscription = SubscriptionName
      .newBuilder
      .setProject(projectId)
      .setSubscription(subscriptionId)
      .build

    // Create subscription if it doesn't exist
    // (TODO) Clean up this pattern
    try {
      subscriptionAdminClient.getSubscription(subscription)
    } catch {
      case e: ApiException => createSubscription(subscriptionId)
    }

    val receiver = new MessageReceiver() {
      override def receiveMessage(message: PubsubMessage, consumer: AckReplyConsumer): Unit = {
        messageStore += message
        log.debug("Received message: " + message.getData.toStringUtf8)
        consumer.ack()
      }
    }
    var subscriber: Subscriber = null
    try {
      subscriber = Subscriber.defaultBuilder(subscription, receiver).build()
      subscriber.addListener(new ApiService.Listener() {
        override def failed(from: ApiService.State, failure: Throwable): Unit = {
          // Handle failure. This is called when the Subscriber encountered a
          // fatal error and is shutting down.
          throw new Exception(failure)
        }
      }, MoreExecutors.directExecutor)
      subscriber.startAsync.awaitRunning()
    } finally if (subscriber != null) {
      subscriber.stopAsync.awaitTerminated()
    }

    messageStore
  }

  def blockUntilNoUndeliveredMessages(subscriptionId: String, metricReceiver: PubsubMetrics) = {
    var topicResponse = metricReceiver
      .getMetric("num_undelivered_messages", subscriptionId)

    while (topicResponse.forall(_ != 0)) {
      Thread.sleep(STACKDRIVER_POLLING_INTERVAL)
      topicResponse = metricReceiver.getMetric("num_undelivered_messages", subscriptionId)
    }
  }
}