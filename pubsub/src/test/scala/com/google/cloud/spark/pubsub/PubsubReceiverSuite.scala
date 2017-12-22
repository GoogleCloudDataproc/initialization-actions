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
import com.google.cloud.pubsub.v1.Subscriber
import org.mockito.Mockito.mock
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.mockito.Mockito.{validateMockitoUsage, verify}
import org.mockito.Mockito.when

/**
 * Unit tests for PubsubReceiver
 */
class PubsubReceiverSuite extends FunSuite with BeforeAndAfterEach {

  var mockSubscriber: Subscriber = null
  var mockService: ApiService = null
  var pubsubReceiver: PubsubReceiver = null

  override def beforeEach: Unit = {
    mockSubscriber = mock(classOf[Subscriber])
    mockService = mock(classOf[ApiService])
    pubsubReceiver = new PubsubReceiver(
      PubsubStreamOptions("projectId", "subscriptionId"),
      (_, _) => mockSubscriber
    )
  }

  override def afterEach: Unit = {
    validateMockitoUsage()
  }

  test("test onStart") {
    // given
    when(mockSubscriber.startAsync()).thenReturn(mockService)

    // when
    pubsubReceiver.onStart()

    // then
    verify(mockSubscriber).startAsync()
    verify(mockService).awaitRunning()
  }

  test("test onStop after onStart") {
    // given
    when(mockSubscriber.startAsync()).thenReturn(mockService)
    when(mockSubscriber.stopAsync()).thenReturn(mockService)
    pubsubReceiver.onStart()

    // when
    pubsubReceiver.onStop()

    // then
    verify(mockSubscriber).stopAsync()
    verify(mockService).awaitTerminated()
  }
}
