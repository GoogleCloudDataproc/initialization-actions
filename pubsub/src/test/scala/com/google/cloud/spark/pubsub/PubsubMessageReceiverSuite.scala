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

import com.google.cloud.pubsub.v1.AckReplyConsumer
import com.google.protobuf.ByteString
import com.google.pubsub.v1.PubsubMessage
import org.mockito.Mockito.mock
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.mockito.Mockito.{validateMockitoUsage, verify}

/**
 * Unit tests for PubsubMessageReceiver
 */
class PubsubMessageReceiverSuite extends FunSuite with BeforeAndAfterEach {
  var mockMessage: PubsubMessage = _
  var mockConsumer: AckReplyConsumer = _
  var mockReceiver: PubsubReceiver = _

  var messageReceiver: PubsubMessageReceiver = _

  override def beforeEach: Unit = {
    mockMessage = PubsubMessage
      .newBuilder
      .setData(ByteString.copyFromUtf8("test"))
      .build
    mockConsumer = mock(classOf[AckReplyConsumer])
    mockReceiver = mock(classOf[PubsubReceiver])

    messageReceiver = new PubsubMessageReceiver(mockReceiver)
  }

  override def afterEach: Unit = {
    validateMockitoUsage()
  }

  test("test receiveMessage") {
    // when
    messageReceiver.receiveMessage(mockMessage, mockConsumer)

    // then
    verify(mockReceiver).store(mockMessage)
    verify(mockConsumer).ack()
  }
}
