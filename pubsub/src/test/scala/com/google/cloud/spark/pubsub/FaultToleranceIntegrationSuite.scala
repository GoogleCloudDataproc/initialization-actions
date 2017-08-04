package com.google.cloud.spark.pubsub

import java.io.File
import java.util.Calendar

import com.google.cloud.ServiceOptions
import com.google.cloud.monitoring.spi.v3.MetricServiceClient
import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.slf4j.LoggerFactory
import com.google.cloud.spark.pubsub.stackdriver.PubsubMetrics

import scala.io.Source

/**
  * Attempting to demonstrate that fault-tolerance
  * works on local filesystem
  *
  */
class FaultToleranceSuite extends FunSuite with BeforeAndAfter {
  val LOG_FOLDER = "src/test/log"
  // (TODO) Remove from src main
  val SOURCE_FILE = "src/test/resources/shakespeare.txt"
  val CHECKPOINT_FOLDER = "src/test/checkpoint"

  val PROJECT_ID = ServiceOptions.getDefaultProjectId
  val TOPIC_ID = "mango" + Calendar.getInstance().getTimeInMillis.toString
  val SUBSCRIPTION_ID = "guava" + Calendar.getInstance().getTimeInMillis.toString

  val client = new PubsubClient(PROJECT_ID, TOPIC_ID)
  val log = LoggerFactory.getLogger(getClass)

  // (TODO) FOLLOWUP Removed in favor of Spark IO
  def countLogLines() = {
    // Read in logged RDDs and compare to input
    // text file by comparing sets of both
    def recursivelyGetFiles(file: File): Array[File] = {
      file.listFiles ++ file.listFiles
        .filter(_.isDirectory)
        .flatMap(recursivelyGetFiles)
    }

    val logFiles = recursivelyGetFiles(new File(LOG_FOLDER))
      .filter(_.isFile)
      .filter(_.getName.startsWith("part-"))
      .toList
      .map(l => l.getAbsolutePath)

    val contentsOfLogs = logFiles
      .flatMap(
        filePath => Source.fromFile(filePath)
          .getLines
          .filter(msg => !msg.isEmpty)
          .toList
      ).toSet

    val messagesFromFile: Set[String] = Source.fromFile(SOURCE_FILE)
      .getLines
      .filter(msg => !msg.isEmpty)
      .toSet

    (messagesFromFile.size, contentsOfLogs.size)
  }

  def createContext(): StreamingContext = {
    val conf = new SparkConf()
      .setAppName("Processing Pubsub")
      .setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(1))
    ssc.checkpoint(CHECKPOINT_FOLDER)

    // Get streaming data from Pub/Sub
    val rddMessages = PubsubUtils.createStream(ssc,
      PROJECT_ID,
      TOPIC_ID,
      SUBSCRIPTION_ID)

    // Write DStream to log to verify against
    rddMessages.map(p => p.getData.toStringUtf8)
      .saveAsTextFiles(LOG_FOLDER + "/output", "txt")

    ssc
  }

  def streamUntilTrue(ssc: StreamingContext, sentinelMethod: () => Unit, gracefulStop: Boolean) = {
    ssc.start()
    sentinelMethod()
    ssc.stop(false, gracefulStop)

  }

  before {
    // Initialize Pub/Sub client and populate with messages
    val fileContents = Source
      .fromFile(SOURCE_FILE)
      .getLines
      .toArray
      .filter(msg => !msg.isEmpty)

    client.createTopic()
    client.createSubscription(SUBSCRIPTION_ID)
    client.publishMessages(fileContents)
  }

  after {
    // Clean up
    FileUtils.deleteDirectory(new File(LOG_FOLDER))
    FileUtils.deleteDirectory(new File(CHECKPOINT_FOLDER))
  }

  test("Fault tolerance works on local file system") {

    // Run stream receiver for short period of
    // time then force quit, restarting should
    // continue streaming from checkpoint
    streamUntilTrue(
        StreamingContext.getOrCreate(
            CHECKPOINT_FOLDER,
            () => createContext()),
        () => Thread.sleep(3000),
        false)
    val (linesInFilePre, linesFromStreamPre) = countLogLines()

    // NOTE: Use Stackdriver to run restarted receiver
    // until Pub/Sub subscription is totally drained
    val metricServiceClient = MetricServiceClient.create()
    val metricReceiver = new PubsubMetrics(PROJECT_ID, metricServiceClient)

    streamUntilTrue(
        StreamingContext.getOrCreate(
            CHECKPOINT_FOLDER,
            () => createContext()),
        () => client.blockUntilNoUndeliveredMessages(SUBSCRIPTION_ID, metricReceiver),
        false)
    val (linesInFilePost, linesFromStreamPost) = countLogLines()

    log.info(s"[PRE] Processed ( $linesFromStreamPre of $linesInFilePre ) records")
    log.info(s"[PST] Processed ( $linesFromStreamPost of $linesInFilePost ) records")

    // Force quit should prevent all messages from being streamed
    assert(linesFromStreamPre < linesInFilePre, "Force terminated after all messages received")

    // Restart from checkpoint should process all messages at-least once
    assert(linesFromStreamPost == linesInFilePost, "Not all messages received")
  }

}
