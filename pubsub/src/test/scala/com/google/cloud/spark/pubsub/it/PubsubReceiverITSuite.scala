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

package com.google.cloud.spark.pubsub.it

import java.nio.file.{Files, Path}

import com.google.cloud.spark.pubsub.{PubsubClient, PubsubUtils, RDDPubsubWriter, TestUtils}
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class PubsubReceiverITSuite extends FunSuite with BeforeAndAfterEach {
  val projectId: String = TestUtils.getProjectId
  val messages: Seq[String] = TestUtils.getTestMessages(100)
  var topicId: String = _
  var subscriptionId: String = _
  var client: PubsubClient = _
  var tempDir: Path = _
  var outputDir: Path = _

  var ssc: StreamingContext = _
  var sc: SparkContext = _

  override def beforeEach: Unit = {
    ssc = TestUtils.getStreamingContext()
    topicId = TestUtils.getTopicId
    subscriptionId = TestUtils.getSubscriptionId
    client = new PubsubClient(projectId, topicId)
    sc = ssc.sparkContext
    tempDir = Files.createTempDirectory("spark-pubsub-test")
    outputDir = tempDir.resolve("output")
    client.createTopic()
  }

  override def afterEach: Unit = {
    client.deleteTopic()
    sc.stop()
  }

  test("Basic write / read test") {
    // Create subscription before publishing to read messages
    client.createSubscription(subscriptionId)

    // publish
    new RDDPubsubWriter(sc.parallelize(messages))
      .writeToPubsub(projectId, topicId)

    // pull
    val inputStream = PubsubUtils.createStream(ssc, projectId, subscriptionId)
    inputStream.map(_.getData.toStringUtf8).saveAsTextFiles(s"$outputDir/part")
    ssc.start()
    Thread.sleep(5 * 1000)
    ssc.stop(stopSparkContext = false)

    val recordedMessages = sc.textFile(s"$outputDir/part-*").collect()
    assertResult(messages.size, "Not all messages received") {recordedMessages.length}
    assert(messages.toSet.diff(recordedMessages.toSet).isEmpty, "Messages are missing")
  }
}
