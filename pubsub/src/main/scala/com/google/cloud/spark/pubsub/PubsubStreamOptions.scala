package com.google.cloud.spark.pubsub

/**
  * Case class to house information about Pub/Sub config
  */
case class PubsubStreamOptions(projectId: String, topicId: String, subscriptionId: String)
