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

import java.util.UUID

import com.google.cloud.ServiceOptions
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{Partition, SparkConf, SparkContext, TaskContext}

import scala.reflect.ClassTag

/**
 * Utility functions for tests.
 */
object TestUtils {
  def getProjectId: String = ServiceOptions.getDefaultProjectId
  def getSubscriptionId: String = s"spark-test-subscription-${UUID.randomUUID()}"
  def getTopicId: String = s"spark-test-topic-${UUID.randomUUID()}"
  def getTestMessages(numMessages: Int): Seq[String] = Range(0, numMessages).map(i => s"test-$i")

  def getStreamingContext(
    conf: Option[SparkConf] = None,
    duration: Duration = Duration(100)): StreamingContext = {
    new StreamingContext(
      conf.getOrElse(
        new SparkConf()
          .setAppName("Spark PubSub Test")
          .setMaster("local[2]")),
      duration)
  }
}

case class TestRDD[T: ClassTag](
  @transient sc: SparkContext, seq: Iterable[T]) extends RDD[T](sc, Nil) {
  case class TestPartition(index: Int) extends Partition

  override def compute(split: Partition, context: TaskContext): Iterator[T] = split match {
    case TestPartition(0) => seq.iterator
  }

  override protected def getPartitions: Array[Partition] = Array(TestPartition(0))

  override def foreachPartition(f: (Iterator[T]) => Unit): Unit = f(seq.iterator)
}
