package com.google.cloud.spark.pubsub

import com.google.cloud.pubsub.spi.v1.AckReplyConsumer
import com.google.protobuf.ByteString
import com.google.pubsub.v1.PubsubMessage
import org.mockito.Mockito.mock
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.mockito.Mockito.{validateMockitoUsage, verify}

/**
  * Unit tests for PubsubMessageReceiver
  */
class PubsubMessageReceiverSuite extends FunSuite with BeforeAndAfter {
  var mockMessage: PubsubMessage = null
  var mockConsumer: AckReplyConsumer = null
  var mockReceiver: PubsubReceiver = null

  var messageReceiver: PubsubMessageReceiver = null

  before {
    mockMessage =  PubsubMessage
      .newBuilder
      .setData(ByteString.copyFromUtf8("test"))
      .build
    mockConsumer = mock(classOf[AckReplyConsumer])
    mockReceiver = mock(classOf[PubsubReceiver])

    messageReceiver = new PubsubMessageReceiver(mockReceiver)
  }

  after {
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
