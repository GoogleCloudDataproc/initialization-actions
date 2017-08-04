package com.google.cloud.spark.pubsub.stackdriver

import com.google.cloud.monitoring.spi.v3.MetricServiceClient
import com.google.cloud.monitoring.spi.v3.PagedResponseWrappers.ListTimeSeriesPagedResponse
import com.google.monitoring.v3.{ListTimeSeriesRequest, ProjectName, TimeInterval, TimeSeries}
import com.google.protobuf.util.Durations
import com.google.protobuf.util.Timestamps

import scala.collection.JavaConverters._
import org.slf4j.LoggerFactory

/**
  * Interact with Stackdriver to get PubSub metrics
  *
  * In order to understand whether subscriptions
  * have been completely cleaned we use Stackdriver
  * metrics such as oldest_unacked_message and
  * num_undelivered_messages
  */
class PubsubMetrics(projectId: String,
                    metricServiceClient: MetricServiceClient,
                   timeSeriesHelper: (MetricServiceClient, ListTimeSeriesRequest) =>
                     ListTimeSeriesPagedResponse = (metricClient, request) =>
                     metricClient.listTimeSeries(request)) {

  private val log = LoggerFactory.getLogger(getClass)

  val name = ProjectName.create(projectId)

  def getMetric(metricName: String, subscriptionId: String): Option[Long] = {
    // Get time window from last minute
    val now = Timestamps.fromMillis(System.currentTimeMillis())
    val timeStep = Durations.fromSeconds(60)

    val interval = TimeInterval.newBuilder()
      .setStartTime(Timestamps.subtract(now, timeStep))
      .setEndTime(now)
      .build()

    val metricType = "pubsub.googleapis.com/subscription/" + metricName
    val requestBuilder = ListTimeSeriesRequest.newBuilder()
      .setNameWithProjectName(name)
      .setFilter(
        s"""metric.type= "$metricType" AND resource.label.subscription_id= "$subscriptionId"""")
      .setInterval(interval)


    var nextToken = ""
    var timeSeries: Iterable[TimeSeries] = null

    // (TODO) Update with PageWrapper when 0.20.2-alpha releases from snapshot
    do {
      if (nextToken != null) {
        requestBuilder.setPageToken(nextToken)
      }
      var request = requestBuilder.build()

      var response = timeSeriesHelper(metricServiceClient, request)

      // (TODO) Consider error handling
      timeSeries = response
        .getPage()
        .getResponse
        .getTimeSeriesList
        .asScala

      nextToken = response.getNextPageToken
    } while (nextToken.nonEmpty)

    if (timeSeries.nonEmpty) {
      // Get last point from returned time series
      val metricValue = timeSeries
        .last
        .getPoints(0)
        .getValue
        .getInt64Value

      return Some(metricValue)
    }
    else {
      return None
    }
  }
}