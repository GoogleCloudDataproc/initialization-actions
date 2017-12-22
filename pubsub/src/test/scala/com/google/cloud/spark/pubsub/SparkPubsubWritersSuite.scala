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

import com.google.api.core.{ApiFuture, ApiFutures}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.{SparkContext, SparkException}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

import scala.collection.mutable

/**
 * Unit tests for SparkPubsubWriters
 */
class SparkPubsubWritersSuite extends FunSuite with BeforeAndAfterEach {

  val messages = TestUtils.getTestMessages(100)
  var ssc: StreamingContext = _
  var sc: SparkContext = _

  override def beforeEach: Unit = {
    ssc = TestUtils.getStreamingContext()
    sc = ssc.sparkContext
  }

  override def afterEach: Unit = {
    if (!sc.isStopped) {
      sc.stop()
    }
  }

  test("test RDD writer") {
    val rddPubsubWriter = new RDDPubsubWriter(sc.parallelize(messages))
    rddPubsubWriter.writeToPubsub(() => new FakePubSubClient())
  }

  test("test RDD writer failure") {
    val rddPubsubWriter = new RDDPubsubWriter(sc.parallelize(messages))
    assertThrows[SparkException] {
      rddPubsubWriter.writeToPubsub(() => new FakePubSubClient(shouldThrow = true))
    }
  }

  test("test DStream writer") {
    val testDStream = ssc.queueStream(mutable.Queue(sc.parallelize(messages)))
    val dStreamPubsubWriter = new DStreamPubsubWriter(testDStream)
    dStreamPubsubWriter.writeToPubsub(() => new FakePubSubClient())
    ssc.start()
    ssc.awaitTerminationOrTimeout(300)
    ssc.stop()
  }

  test("test DStream writer failure") {
    val testDStream = ssc.queueStream(mutable.Queue(sc.parallelize(messages)))
    val dStreamPubsubWriter = new DStreamPubsubWriter(testDStream)
    dStreamPubsubWriter.writeToPubsub(() => new FakePubSubClient(shouldThrow = true))
    assertThrows[SparkException] {
      ssc.start()
      ssc.awaitTerminationOrTimeout(300)
    }
    ssc.stop()
  }
}

class FakePubSubClient(shouldThrow: Boolean = false)
  extends PubsubClient("projectId", "topicId") {

  override def publishMessages(messages: TraversableOnce[String]): Seq[ApiFuture[String]] = {
    messages.map(response).toSeq
  }

  private def response(message: String): ApiFuture[String] = {
    if (shouldThrow) {
      return ApiFutures.immediateFailedFuture[String](new Exception("pubsub error"))
    }
    ApiFutures.immediateFuture(message)
  }
}
