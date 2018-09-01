# Spark Pub/Sub Connector (Beta)

The Apache Spark Pub/Sub Connector exposes Google Cloud Pub/Sub as a Spark Streaming source in an
idiomatic way. Instead of the user working with the details of the Pub/Sub API, they simply create
a DStream object for a Pub/Sub topic and the connector handles the details of creating a
subscription to the data. Ultimately, users will be able to interact with Pub/Sub as such in
Spark in a streaming context.

## Important Note on Reliability

This connector currently does *not* guarantee the at least once semantics of a [Reliable
Receiver](http://spark.apache.org/docs/latest/streaming-custom-receivers.html#receiver-reliability).
This receiver ACKs messages as soon as it calls `Receiver.store` on a single
message. Spark buffers that message in memory before flushing to HDFS, and it
could be lost in the event of a lost Executor. There is work in progress to
remedy this.

This connector does not provide at most once semantics, though the user may
attempt to filter out duplicates as shown in the
[MinuteRideAggregator](src/main/scala/com/google/cloud/spark/pubsub/examples/MinuteRideAggregator.scala)

## Quick Use

To create a `DStream` and immediately begin using the connector, simply add the following line

```scala
val rddMessages = PubsubUtils.createStream(ssc, projectId, topicId, subscriptionId)
```

Multiple Pubsub input DStreams can be created and union-ed for parallel receiving of data from
multiple receivers.

```scala
 val singleStreams = (0 until numReceivers).map(stream => {
   PubsubUtils.createStream(ssc, projectId, topicId, subscriptionId)
 })
 val rddMessages = ssc.union(singleStreams)
```

## Building and Testing

The connector can be built with Maven or SBT.

```bash
mvn package
sbt assembly
```

### Running Integration tests

The integration tests are only configured for SBT. To run the integration tests, first enable Google
Application Credentials.

```bash
gcloud auth application-default login
```

Then run the `it:test` task.

```bash
sbt it:test
```

## Running the NYC Taxi Example

This will stream data from the NYC Taxi Pub/Sub topic through the connector on a Dataproc cluster,
perform some Spark analysis using mapWithState to continuously monitor revenue per minute and
output the final revenue/minute once StackDriver reports oldest message time has passed, then
write the results to a GCS (Google Cloud Storage) bucket.

Note, the bulk of this guide is predicated on having the [gcloud](https://cloud.google.com/sdk/gcloud/)
CLI installed.

### Build project uber-JAR

Build uber-JAR with SBT or Maven as [described above](#building-and-testing)

```bash
sbt clean assembly
```

### Create Dataproc cluster, GCS bucket, and Cloud Pub/Sub Subscription

The demo will be running in parallel on Dataproc and writing to GCS so these need to be set up.
To create a Dataproc cluster and GCS bucket

```bash
CLUSTER=<cluster-name>
gcloud dataproc clusters create "${CLUSTER}" --scopes=cloud-platform --num-workers=30
```

```bash
BUCKET=<bucket-name>
gsutil mb "gs://${BUCKET}/"
```

```bash
SUBSCRIPTION='test-taxi-sub'
gcloud alpha pubsub subscriptions create "${SUBSCRIPTION}" \
    --topic projects/pubsub-public-data/topics/taxirides-realtime
```
### Submit NYC Taxi Demo to Spark on Dataproc

Now that we have a far jar of the project and all the cloud components correctly initialized,
submit the job to the cluster with the correct arguments. This NYC Taxi demo takes two arguments:
(1) a log directory to view the job output and (2) a checkpoint directory for the Write-ahead Log
(WAL) and fault-tolerance. Note that the WAL (checkpoint) has to point to a HDFS location.

```bash
CHECKPOINT_DIR="hdfs:/tmp/checkpoint"
gcloud dataproc jobs submit spark --cluster "${CLUSTER}" \
    --jars target/scala-2.11/spark-pubsub-assembly-0.1.0-SNAPSHOT.jar \
    --class com.google.cloud.spark.pubsub.examples.MinuteRideAggregator \
    -- "gs://${BUCKET}/logs" "${CHECKPOINT_DIR}" "${SUBSCRIPTION}"
```

You can monitor the job and the real-time output of the demo using the GCloud GUI for Dataproc and
Storage, respectively.

### (Optional) Set up Spark UI port forwarding

For more granular metrics on the Spark Streaming job, the Spark UI is excellent but does not
have immediate access for clusters for running on Dataproc. Fortunately, there is a [guide](https://cloud.google.com/dataproc/docs/concepts/cluster-web-interfaces) to set it up.
In short, run the following two commands noting that we add the master suffix for the cluster name
and that the executive path for Chrome depends on your OS

```bash
gcloud compute --project "<project-name>" ssh --zone "<zone>" --ssh-flag="-D 1080" "<cluster-name>-m"
<path-to-chrome> --proxy-server="socks5://localhost:1080" --host-resolver-rules="MAP * 0.0.0.0 , EXCLUDE localhost" --user-data-dir=/tmp/
```

In the Chrome browser that opens navigate to `http://master-host-name:8088` if on YARN or
`http://master-host-name:50070` if on HDFS. Move to the Streaming on the far right to see
granular metrics such as Input Rate and Processing Time.
