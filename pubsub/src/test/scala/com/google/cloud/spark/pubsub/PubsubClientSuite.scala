package com.google.cloud.spark.pubsub

import com.google.cloud.monitoring.spi.v3.MetricServiceClient
import com.google.cloud.spark.pubsub.stackdriver.PubsubMetrics
import org.mockito.Mockito.mock
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.mockito.Mockito.{validateMockitoUsage, verify}
import org.mockito.Mockito.when

/**
  * Unit tests for PubsubClient
  */
class PubsubClientSuite extends FunSuite with BeforeAndAfter {

  var mockMetricReceiver: PubsubMetrics = null
  var pubsubClient: PubsubClient = null

  before {
    mockMetricReceiver = mock(classOf[PubsubMetrics])
    pubsubClient = new PubsubClient("123", "456")
  }

  after {
    validateMockitoUsage()
  }

  test("test call to getMetric") {
    // when
    pubsubClient.blockUntilNoUndeliveredMessages("789", mockMetricReceiver)

    // then
    verify(mockMetricReceiver).getMetric("num_undelivered_messages", "789")
  }
}
