package com.google.cloud.spark.pubsub.examples

import java.nio.file.Files
import java.util.Calendar

import com.google.cloud.ServiceOptions
import com.google.cloud.spark.pubsub.{PubsubClient, PubsubUtils}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory

object PubsubReceiverDriver {
  val LOG_FOLDER = Files.createTempDirectory("pubsub-spark").toFile
  val PROJECT_ID = ServiceOptions.getDefaultProjectId
  val TOPIC_ID = "mango" + Calendar.getInstance().getTimeInMillis.toString
  val SUBSCRIPTION_ID = "guava" + Calendar.getInstance().getTimeInMillis.toString

  val client = new PubsubClient(PROJECT_ID, TOPIC_ID)

  private val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val sourceFilePath = args(0)
    val logFolderPath = args(1)

    val conf = new SparkConf()
      .setAppName("Processing Pubsub")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(1))

    // Initialize Pub/Sub client and populate
    // with messages
    client.createTopic()
    client.createSubscription(SUBSCRIPTION_ID)
    client.publishMessages(sc
        .textFile(sourceFilePath)
        .collect()
    )

    // Get streaming data from Pub/Sub
    val rddMessages = PubsubUtils.createStream(ssc, PROJECT_ID, TOPIC_ID, SUBSCRIPTION_ID)

    // Write DStream to log to verify against
    rddMessages.map(p => p.getData.toStringUtf8)
      .saveAsTextFiles(logFolderPath + "/output", "txt")

    ssc.start()
    Thread.sleep(60000)
    ssc.stop()
  }
}