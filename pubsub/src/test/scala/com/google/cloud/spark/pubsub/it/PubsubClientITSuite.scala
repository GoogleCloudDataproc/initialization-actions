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

package com.google.cloud.spark.pubsub.it

import com.google.cloud.pubsub.v1.SubscriptionAdminClient
import com.google.cloud.spark.pubsub.{PubsubClient, TestUtils}
import com.google.pubsub.v1.SubscriptionName
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class PubsubClientITSuite extends FunSuite with BeforeAndAfterEach {
  val projectId: String = TestUtils.getProjectId
  var topicId: String = _
  var subscriptionId: String = _
  val messages: Set[String] = TestUtils.getTestMessages(1000).toSet

  var client: PubsubClient = _

  override def beforeEach: Unit = {
    topicId = TestUtils.getTopicId
    subscriptionId = TestUtils.getTopicId
    client = new PubsubClient(projectId, topicId)
    client.createTopic()
  }

  override def afterEach: Unit = {
    client.deleteTopic()
  }

  test("Creating and deleting topic") {
    // covered by before & after
  }

  test("Writing messages to topic") {
    val msgIDs = client.publishMessages(messages)
    assert(msgIDs.size == messages.size)
  }

  test("Creating subscription") {
    client.createSubscription(subscriptionId)
    val subscription: SubscriptionName = SubscriptionName
      .newBuilder
      .setProject(projectId)
      .setSubscription(subscriptionId)
      .build

    // Will throw Exception on error
    SubscriptionAdminClient.create.getSubscription(subscription)
  }
}
