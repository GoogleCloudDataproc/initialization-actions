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

import com.google.cloud.spark.pubsub.stackdriver.PubsubMetrics
import org.mockito.Matchers.any
import org.mockito.Mockito.{mock, times, validateMockitoUsage, verify, when}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

/**
 * Unit tests for PubsubClient
 */
class PubsubClientSuite extends FunSuite with BeforeAndAfterEach {

  var mockMetricReceiver: PubsubMetrics = _
  var pubsubClient: PubsubClient = _

  val project = "test-project"
  val topic = "test-topic"
  val subscription = "test-subscription"

  override def beforeEach: Unit = {
    mockMetricReceiver = mock(classOf[PubsubMetrics])
    pubsubClient = new PubsubClient(project, topic, stackDriverPollingInterval = 0)
  }

  override def afterEach: Unit = {
    validateMockitoUsage()
  }

  test("test call to getMetric") {
    when(
      mockMetricReceiver
        .getMetric(any(classOf[String]), any(classOf[String])))
        .thenReturn(Some(5L), Some(2L), Some(0L))
    pubsubClient.blockUntilNoUndeliveredMessages(subscription, mockMetricReceiver)

    // then
    verify(mockMetricReceiver, times(3)).getMetric("num_undelivered_messages", subscription)
  }
}
