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

import com.google.cloud.spark.pubsub.stackdriver.PubsubMetrics
import com.google.cloud.spark.pubsub.{PubsubClient, PubsubUtils, TestUtils}
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.slf4j.{Logger, LoggerFactory}

/**
 * Attempting to demonstrate that fault-tolerance
 * works on local filesystem
 *
 */
class FaultToleranceITSuite extends FunSuite with BeforeAndAfterEach {
  val projectId: String = TestUtils.getProjectId
  var topicId: String = _
  var subscriptionId: String = _
  val messages: Set[String] = TestUtils.getTestMessages(100 * 1000).toSet

  var client: PubsubClient = _
  val log: Logger = LoggerFactory.getLogger(getClass)
  var tempDir: Path = _
  var checkpointDir: String = _
  var outputDir: String = _

  var ssc: StreamingContext = _
  var sc: SparkContext = _

  override def beforeEach: Unit = {
    topicId = TestUtils.getTopicId
    subscriptionId = TestUtils.getTopicId

    sc = TestUtils.getStreamingContext().sparkContext
    tempDir = Files.createTempDirectory("spark-pubsub-test")
    checkpointDir = tempDir.resolve("checkpoint").toString
    outputDir = tempDir.resolve("output").toString

    client = new PubsubClient(projectId, topicId)
    client.createTopic()
  }

  override def afterEach: Unit = {
    client.deleteTopic()
    sc.stop()
  }

  def createContext(): StreamingContext = {
    val _ssc = new StreamingContext(sc, Seconds(1))
    _ssc.checkpoint(checkpointDir.toString)

    val inputStream = PubsubUtils.createStream(_ssc, projectId, subscriptionId)
    inputStream.map(_.getData.toStringUtf8).saveAsTextFiles(s"$outputDir/part")

    _ssc
  }

  test("Fault tolerance works on local file system") {
    client.createSubscription(subscriptionId)
    client.publishMessages(messages)

    ssc = StreamingContext.getOrCreate(checkpointDir.toString, () => createContext())

    // Run stream receiver for short period of
    // time then force quit, restarting should
    // continue streaming from checkpoint
    ssc.start()
    Thread.sleep(3000)
    ssc.stop(stopSparkContext = false, stopGracefully = false)
    val partialMessagesProcessed = sc.textFile(s"$outputDir/part-*").count()

    // NOTE: Use Stackdriver to run restarted receiver
    // until Pub/Sub subscription is totally drained
    val metricReceiver = new PubsubMetrics(projectId)

    ssc = StreamingContext.getOrCreate(checkpointDir.toString, () => createContext())
    ssc.start()
    client.blockUntilNoUndeliveredMessages(subscriptionId, metricReceiver)
    ssc.stop(stopSparkContext = false, stopGracefully = true)
    val receivedMessages = sc.textFile(s"$outputDir/part-*").collect()

    log.info(s"Initially processed $partialMessagesProcessed of ${messages.size} records")
    log.info(s"Finally processed  ${receivedMessages.size} of ${messages.size} records")

    // Force quit should prevent all messages from being streamed
    assert(
      partialMessagesProcessed < receivedMessages.size,
      "Force terminated after all messages received")

    // Restart from checkpoint should process all messages at-least once
    assert(receivedMessages.toSet.diff(messages).isEmpty, "Not all messages received")
  }

}
