package com.google.cloud.spark.pubsub

import java.io.File
import java.util.Calendar

import com.google.cloud.ServiceOptions
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.scalatest.FunSuite
import org.slf4j.LoggerFactory
import org.apache.commons.io.FileUtils

import scala.io.Source

class PubsubReceiverIntegrationSuite extends FunSuite {
  val LOG_FOLDER = new File("src/temp")
  val PROJECT_ID = ServiceOptions.getDefaultProjectId
  val TOPIC_ID = "mango" + Calendar.getInstance().getTimeInMillis.toString
  val SUBSCRIPTION_ID = "guava" + Calendar.getInstance().getTimeInMillis.toString
  val SOURCE_FILE = "src/test/resources/shakespeare.txt"

  val client = new PubsubClient(PROJECT_ID, TOPIC_ID)

  private val log = LoggerFactory.getLogger(getClass)

  test("At least once streaming semantics succeeds") {
    /*
    * NOTE: This test is also meant to function as a
    * demo of the core streaming functionality of the
    * connector, thus it is slightly longer and more
    * complex than the other tests
     */

    LOG_FOLDER.mkdir

    val conf = new SparkConf()
      .setAppName("Processing Pubsub")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(1))

    val fileContents = Source
        .fromURL(SOURCE_FILE)
        .getLines
        .filter(msg => !msg.isEmpty)
        .toArray

    // Initialize Pub/Sub client and populate
    // with messages
    client.createTopic()
    client.createSubscription(SUBSCRIPTION_ID)
    client.publishMessages(fileContents)

    // Get streaming data from Pub/Sub
    val rddMessages = PubsubUtils.createStream(ssc, PROJECT_ID, TOPIC_ID, SUBSCRIPTION_ID)

    // Write DStream to log to verify against
    rddMessages.map(p => p.getData.toStringUtf8)
      .saveAsTextFiles("file://" + LOG_FOLDER.getAbsolutePath + "/output", "txt")

    ssc.start()
    Thread.sleep(60000)
    ssc.stop()

    // Read in logged RDDs and compare to input
    // text file by comparing sets of both
    def recursivelyGetFiles(file: File): Array[File] = {
      file.listFiles ++ file.listFiles
        .filter(_.isDirectory)
        .flatMap(recursivelyGetFiles)
    }

    val logFiles = recursivelyGetFiles(LOG_FOLDER)
      .filter(_.isFile)
      .filter(_.getName.startsWith("part-"))
      .toList
      .map(l => l.getAbsolutePath)

    val contentsOfLogs = logFiles
      .flatMap(
        filePath => Source.fromFile(filePath)
          .getLines
          .filter(msg => !msg.isEmpty)
      ).toSet

    val messagesFromFile: Set[String] = Source.fromFile(SOURCE_FILE.getPath)
      .getLines
      .filter(msg => !msg.isEmpty)
      .toSet

    // At-least once semantics means we could have duplicate
    // messages but are guaranteed at least one of each
    // However, taking both lists as sets should guarantee equality
    assert(contentsOfLogs.size == messagesFromFile.size)

    // Clean up
    FileUtils.deleteDirectory(LOG_FOLDER)
  }
}
