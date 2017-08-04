package com.google.cloud.spark.pubsub.stackdriver
import com.google.api.core.ApiFutures
import com.google.api.gax.grpc.{PageContext, PagedListDescriptor}
import com.google.cloud.monitoring.spi.v3.MetricServiceClient
import com.google.cloud.monitoring.spi.v3.PagedResponseWrappers.ListTimeSeriesPagedResponse
import org.slf4j.LoggerFactory
import com.google.monitoring.v3.{ListTimeSeriesRequest, ListTimeSeriesResponse}
import com.google.monitoring.v3.{TimeSeries, Point, TypedValue}
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.mockito.Mockito.{mock, validateMockitoUsage, when}

import scala.collection.JavaConverters._

class PubsubMetricsSuite extends FunSuite with BeforeAndAfter {
  val log = LoggerFactory.getLogger(getClass)

  var mockMetricClient: MetricServiceClient = null
  var mockRequest: ListTimeSeriesRequest = null
  var mockContext: PageContext[ListTimeSeriesRequest,
    ListTimeSeriesResponse,
    TimeSeries] = null
  var mockDescription: PagedListDescriptor[ListTimeSeriesRequest,
    ListTimeSeriesResponse,
    TimeSeries] = null

  var expectedResponse: ListTimeSeriesResponse = null
  var pubsubMetrics: PubsubMetrics = null

  before {
    mockMetricClient = mock(classOf[MetricServiceClient])
    mockContext = mock(classOf[PageContext[ListTimeSeriesRequest,
      ListTimeSeriesResponse,
      TimeSeries]])
    mockDescription = mock(classOf[PagedListDescriptor[ListTimeSeriesRequest,
      ListTimeSeriesResponse,
      TimeSeries]])
    mockRequest = ListTimeSeriesRequest.newBuilder().build()

    val timeSeriesElement = TimeSeries
      .newBuilder()
      .addPoints(Point
        .newBuilder()
        .setValue(TypedValue.newBuilder()
          .setInt64Value(20)))
      .build()

    val timeSeries = List(timeSeriesElement).asJava
    expectedResponse = ListTimeSeriesResponse
      .newBuilder
      .addAllTimeSeries(timeSeries)
      .build

    val responseObject = ListTimeSeriesPagedResponse
      .createAsync(mockContext, ApiFutures.immediateFuture(expectedResponse)).get()

    pubsubMetrics = new PubsubMetrics("123", mockMetricClient, (_, _) => responseObject)
  }

  test("Request is correctly parsed for Point value") {
    when(mockContext.getPageDescriptor).thenReturn(mockDescription)
    when(mockDescription.extractNextToken(expectedResponse)).thenReturn("")

    val returnedMetric = pubsubMetrics.getMetric("789", "456")
    assert(returnedMetric == 20)
  }

  after {
    validateMockitoUsage()
  }

}

