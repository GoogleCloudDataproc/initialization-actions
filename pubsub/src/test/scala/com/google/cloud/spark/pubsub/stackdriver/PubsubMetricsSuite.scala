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
import com.google.monitoring.v3._
import org.mockito.Mockito.{mock, validateMockitoUsage}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class PubsubMetricsSuite extends FunSuite with BeforeAndAfterEach {

  var mockMetricClient: MetricServiceClient = _

  def fakeListTimeSeries(
    expectedProject: ProjectName,
    expectedFilter: String,
    timeSeries: Iterable[TimeSeries])(
    client: MetricServiceClient,
    request: ListTimeSeriesRequest): Iterable[TimeSeries] = {
    assert(expectedProject.equals(request.getNameAsProjectName))
    assert(expectedFilter.equals(request.getFilter))
    assert(request.getInterval != null)
    timeSeries
  }

  override def beforeEach: Unit = {
    mockMetricClient = mock(classOf[MetricServiceClient])
  }

  override def afterEach: Unit = {
    validateMockitoUsage()
  }

  test("Request is correctly parsed for Point value") {
    val project = "test-project"
    val subscription = "test-subscription"
    val metric = "test-metric"
    val filter =
      """metric.type="pubsub.googleapis.com/subscription/test-metric"
        |AND resource.label.subscription_id="test-subscription""""
        .stripMargin.replaceAll("\n", " ")

    val timeSeries =
      TimeSeries
        .newBuilder()
        .addPoints(Point
          .newBuilder()
          .setValue(TypedValue.newBuilder()
            .setInt64Value(20)))
        .build()

    val pubsubMetrics = new PubsubMetrics(
      project,
      () => mockMetricClient,
      fakeListTimeSeries(ProjectName.create(project), filter, Seq(timeSeries)))

    val returnedMetric = pubsubMetrics.getMetric(metric, subscription)
    assertResult(Some(20)) {
      returnedMetric
    }
  }
}

