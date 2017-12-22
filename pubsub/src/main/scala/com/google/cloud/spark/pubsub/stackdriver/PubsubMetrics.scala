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

package com.google.cloud.spark.pubsub.stackdriver

import com.google.cloud.monitoring.v3.MetricServiceClient
import com.google.monitoring.v3.{ListTimeSeriesRequest, ProjectName, TimeInterval, TimeSeries}
import com.google.protobuf.util.{Durations, Timestamps}

import scala.collection.JavaConverters._

/**
 * Interact with Stackdriver to get PubSub metrics
 *
 * In order to understand whether subscriptions
 * have been completely cleaned we use Stackdriver
 * metrics such as oldest_unacked_message and
 * num_undelivered_messages
 */
class PubsubMetrics(
  projectId: String,
  metricServiceClientProvider: () => MetricServiceClient = MetricServiceClient.create,
  listTimeSeries:
    (MetricServiceClient, ListTimeSeriesRequest) => Iterable[TimeSeries] =
    PubsubMetrics.listTimesSeries) {

  val name = ProjectName.create(projectId)
  val metricClient: MetricServiceClient = metricServiceClientProvider()

  def getMetric(metricName: String, subscriptionId: String): Option[Long] = {
    // Get time window from last minute
    val now = Timestamps.fromMillis(System.currentTimeMillis())
    val timeStep = Durations.fromSeconds(60)

    val interval = TimeInterval.newBuilder()
      .setStartTime(Timestamps.subtract(now, timeStep))
      .setEndTime(now)
      .build()

    val metricType = "pubsub.googleapis.com/subscription/" + metricName
    val request = ListTimeSeriesRequest.newBuilder()
      .setNameWithProjectName(name)
      .setFilter(
        s"""metric.type="$metricType" AND resource.label.subscription_id="$subscriptionId"""")
      .setInterval(interval)
      .build()

    listTimeSeries(metricClient, request)
      .lastOption
      .map(_.getPoints(0).getValue.getInt64Value)
  }
}

object PubsubMetrics {
  def listTimesSeries(
    client: MetricServiceClient,
    request: ListTimeSeriesRequest): Iterable[TimeSeries] = {
    client.listTimeSeries(request).iterateAll.asScala
  }
}
