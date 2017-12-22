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

package com.google.cloud.spark.pubsub.examples

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.google.cloud.ServiceOptions
import com.google.cloud.spark.pubsub.stackdriver.PubsubMetrics
import com.google.cloud.spark.pubsub.{PubsubClient, PubsubUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import scala.util.parsing.json.JSON


/**
 * Aggregates real-time revenue from taxi rides in NYC by minute
 *
 * To demonstrate the functionality of this Pub/Sub connector
 * and common analysis patterns in Spark, data is streamed
 * from the NYC Taxi Pub/Sub topic. First each entry is mapped
 * to its meter increment then using updateStateByKey is aggregated
 * by the minute of the timestamp
 */
object MinuteRideAggregator {
  val PROJECT_ID = ServiceOptions.getDefaultProjectId

  val WINDOW_SIZE = Seconds(180)
  val SLIDE_INTERVAL = Seconds(180)
  val CHECKPOINT_INTERVAL = Seconds(360)

  val metricReceiver = new PubsubMetrics(PROJECT_ID)

  private val log = LoggerFactory.getLogger(getClass)

  // Convert JSON messages to map
  def parseRideJson(rideMessage: String): Map[String, Any] = {
    val taxiDataRaw = JSON.parseFull(rideMessage)

    taxiDataRaw match {
      case Some(map: Map[String, Any]) => map
      case _ => sys.error("Invalid message format, could not convert to map")
    }
  }

  def getMinuteRevenue(ride: Map[String, Any]): (Date, Double) = {
    val timestamp = ride("timestamp").asInstanceOf[String].split("\\.")(0)
    val increment = ride("meter_increment").asInstanceOf[Double]
    val calendarParse = Calendar.getInstance

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss")
    val parsedDate = dateFormat.parse(timestamp)

    calendarParse.setTime(parsedDate)
    calendarParse.set(Calendar.SECOND, 0)

    (calendarParse.getTime, increment)
  }

  def updateCumulativeRevenue(values: Seq[Double], state: Option[Double]): Option[Double] = {
    val currentCount = values.sum
    val previousCount = state.getOrElse(0.0)
    Some(currentCount + previousCount)
  }


  def filterFinishedTime(date: Date, subscriptionId: String): Boolean = {
    // (TODO) Update timestamp usage to publish time
    val oldestUnackedMessageAge = metricReceiver
      .getMetric("oldest_unacked_message_age", subscriptionId).getOrElse(0L)

    val calendarRewind = Calendar.getInstance

    calendarRewind.setTime(new Date())
    calendarRewind.add(Calendar.SECOND, -1 * oldestUnackedMessageAge.toInt)
    calendarRewind.add(Calendar.HOUR, 3) // (TODO) Replace with TimeZone handling for EST
    calendarRewind.set(Calendar.SECOND, 0)

    val oldestUnackedMessage = calendarRewind.getTime

    date.before(oldestUnackedMessage)
  }

  def createContextAndAggregateMinute(
      checkpointDirectory: String,
      outputFolderPath: String,
      subscriptionId: String): StreamingContext = {
    val conf = new SparkConf()
      .setAppName("Processing Pubsub")
      .set("spark.streaming.receiver.writeAheadLog.enable", "true")
      .set("spark.streaming.driver.writeAheadLog.allowBatching", "true")
      .set("spark.streaming.driver.writeAheadLog.batchingTimeout", "15000")

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, SLIDE_INTERVAL)
    ssc.checkpoint(checkpointDirectory)

    val updatedMins = PubsubUtils.createStream(ssc, PROJECT_ID, subscriptionId)
      .checkpoint(CHECKPOINT_INTERVAL)
      .window(WINDOW_SIZE, SLIDE_INTERVAL)
      .map(pmessage => pmessage.getData.toStringUtf8)
      .map(message => parseRideJson(message))
      .map(message => getMinuteRevenue(message))
      .updateStateByKey(updateCumulativeRevenue)

    updatedMins
      .filter(pair => filterFinishedTime(pair._1, subscriptionId))
      .foreachRDD(r => r.coalesce(20).saveAsTextFile(outputFolderPath + "/aggregate"))

    ssc
  }

  def main(args: Array[String]): Unit = {
    assert(args.length == 3, "Invalid number of arguments provided")

    val outputFolderPath = args(0)
    val checkpointDirectory = args(1)
    val subscriptionId = args(2)


    val ssc = StreamingContext.getOrCreate(
      checkpointDirectory,
      () => createContextAndAggregateMinute(checkpointDirectory, outputFolderPath, subscriptionId)
    )

    ssc.start()
    ssc.awaitTermination()
  }
}
