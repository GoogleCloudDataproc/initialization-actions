package com.google.cloud.spark.pubsub

import com.google.api.core.ApiService
import com.google.cloud.pubsub.spi.v1.Subscriber
import org.mockito.Mockito.mock
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.mockito.Mockito.{verify, validateMockitoUsage}
import org.mockito.Mockito.when

/**
  * Unit tests for PubsubReceiver
  */
class PubsubReceiverSuite extends FunSuite with BeforeAndAfter {

  var mockSubscriber: Subscriber = null
  var mockService: ApiService = null
  var pubsubReceiver: PubsubReceiver = null

  before {
    mockSubscriber = mock(classOf[Subscriber])
    mockService = mock(classOf[ApiService])
    pubsubReceiver = new PubsubReceiver(
      PubsubStreamOptions("projectId", "topicId", "subscriptionId"),
      (_, _) => mockSubscriber
    )
  }

  after {
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
