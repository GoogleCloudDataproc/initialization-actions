package com.google.cloud.spark.pubsub

import com.google.api.core.ApiService
import com.google.cloud.pubsub.spi.v1.Subscriber
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.{SparkConf, SparkContext}
import org.mockito.Mockito.{mock, validateMockitoUsage, verify, when}
import org.scalatest.{BeforeAndAfter, FunSuite}
import com.google.cloud.spark.pubsub.DStreamPubsubWriter._
import com.google.cloud.spark.pubsub.RDDPubsubWriter._

import scala.collection.mutable

/**
  * Unit tests for SparkPubsubWriters
  */
class SparkPubsubWritersSuite extends FunSuite with BeforeAndAfter {

  var mockClient: PubsubClient = null

  val ssc = new StreamingContext(
    new SparkConf()
      .setAppName("SparkPubsubWritersSuite")
      .setMaster("local"),
    Duration(100)
  )
  val sc = ssc.sparkContext

  var testRdd: RDD[String] = null
  var testDstream: DStream[String] = null

  val alpha1 = Seq("A", "B", "C")
  val alpha2 = Seq("D", "E", "F")

  before {
    mockClient = mock(classOf[PubsubClient])
  }

  after {
    validateMockitoUsage()
  }

  test("test RDD writer") {
    testRdd = sc.parallelize(alpha1)
    val rDDPubsubWriter = new RDDPubsubWriter(testRdd)

    // when
    rDDPubsubWriter.writeToPubsub(mockClient)

    // then
    verify(mockClient).publishMessages(alpha1.toArray)
  }


  test("test DStream writer") {
    testDstream = ssc.queueStream(
      mutable.Queue(sc.parallelize(alpha2))
    )
    val dStreamPubsubWriter = new DStreamPubsubWriter(testDstream)

    // when
    dStreamPubsubWriter.writeToPubsub(mockClient)

    ssc.start()
    ssc.awaitTerminationOrTimeout(100)

    // then
    verify(mockClient).publishMessages(alpha2.toArray)
  }


}
