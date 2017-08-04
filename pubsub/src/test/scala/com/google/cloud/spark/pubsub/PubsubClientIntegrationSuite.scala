package com.google.cloud.spark.pubsub

import java.util.Calendar

import com.google.cloud.pubsub.spi.v1.SubscriptionAdminClient
import com.google.pubsub.v1.SubscriptionName
import org.scalatest.FunSuite

import scala.io.Source

class PubsubClientIntegrationSuite extends FunSuite {
  val PROJECT_ID = ServiceOptions.getDefaultProjectId
  val TOPIC_ID = "mango" + Calendar.getInstance().getTimeInMillis.toString
  val SUBSCRIPTION_ID = "guava" + Calendar.getInstance().getTimeInMillis.toString
  val SOURCE_FILE = "src/test/resources/shakespeare.txt"

  val client = new PubsubClient(PROJECT_ID, TOPIC_ID)

  test("Creating topic") {
    // Will throw Exception on error
    client.createTopic()
  }

  test("Deleting topic") {
    // Will throw Exception on error
    client.deleteTopic()
  }

  test("Writing messages to topic") {
    client.createTopic()

    val fileContents: Array[String] = Source
      .fromURL(SOURCE_FILE)
      .getLines
      .filter(msg => !msg.isEmpty)
      .toArray


    val msgIDs = client.publishMessages(fileContents)
    assert(msgIDs.size == fileContents.size)

    client.deleteTopic()
  }

  test("Creating subscription") {
    client.createTopic()

    client.createSubscription(SUBSCRIPTION_ID)
    val subscription: SubscriptionName = SubscriptionName
      .newBuilder
      .setProject(PROJECT_ID)
      .setSubscription(SUBSCRIPTION_ID)
      .build

    // Will throw Exception on error
    SubscriptionAdminClient.create.getSubscription(subscription)

    client.deleteTopic()
  }

  test("Getting messages from topic") {
    client.createTopic()
    val fileContents: Array[String] = Source
      .fromURL(SOURCE_FILE)
      .getLines
      .filter(msg => !msg.isEmpty)
      .toArray

    client.publishMessages(fileContents)

    // Will throw Exception on error
    val messagesFromTopic = client.getMessagesFromTopic(SUBSCRIPTION_ID)

    client.deleteTopic()
  }
}
