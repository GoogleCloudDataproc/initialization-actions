# Spark Pub/Sub Connector
The Apache Spark Pub/Sub Connector exposes Google Cloud Pub/Sub as a Spark Streaming source in an
idiomatic way. Instead of the user working with the details of the Pub/Sub API, they simply create
a DStream object for a Pub/Sub topic and the connector handles the details of creating a
subscription to the data. Ultimately, users will be able to interact with Pub/Sub as such in
Spark in a streaming context.


## Quick Use
To create a `DStream` and immediately begin using the connector, simply add the following line

```val rddMessages = PubsubUtils.createStream(ssc, projectId, topicId, subscriptionId)```

Multiple Pubsub input DStreams can be created and union-ed for parallel receiving of data from
multiple receivers.

```
 val singleStreams = (0 until numReceivers).map(stream => {
   PubsubUtils.createStream(ssc, projectId, topicId, subscriptionId)
 })
 val rddMessages = ssc.union(singleStreams)
```

## Running the NYC Taxi Example
This will stream data from the NYC Taxi Pub/Sub topic through the connector on a Dataproc cluster,
perform some Spark analysis using mapWithState to continuously monitor revenue per minute and
output the final revenue/minute once StackDriver reports oldest message time has passed, then
write the results to a GCS (Google Cloud Storage) container.

Note, the bulk of this guide is predicated on having the [gcloud](https://cloud.google.com/sdk/gcloud/)
CLI installed.

### Build project uber-jar
To build a uber-jar to submit to Spark, clone the source project and sbt and run

```./build/sbt 'set test in assembly := {}' clean assembly```

### Create Dataproc cluster and GCS bucket
The demo will be running in parallel on Dataproc and writing to GCS so these need to be set up.
To create a Dataproc cluster ((TODO) Set cluster count to 30) and GCS bucket

```gcloud dataproc clusters create <cluster-name> --scopes=cloud-platform```

```gsutil mb gs://<bucket-name>/```

### Submit NYC Taxi Demo to Spark on Dataproc
Now that we have a far jar of the project and all the cloud components correctly initialized,
submit the job to the cluster with the correct arguments. This NYC Taxi demo takes two arguments:
(1) a log directory to view the job output and (2) a checkpoint directory for the Write-ahead Log
(WAL) and fault-tolerance. Note that the WAL (checkpoint) has to point to a HDFS location.

```
gcloud dataproc jobs submit spark --cluster <cluster-name> --jar \\
    target/scala-2.11/spark-pubsub-assembly-0.1.0.jar \\
    -- gs://<bucket-name>/logs hdfs:/<dir>/checkpoint
```

You can monitor the job and the real-time output of the demo using the GCloud GUI for Dataproc and
Storage, respectively.

### (Optional) Set up Spark UI port forwarding
For more granular metrics on the Spark Streaming job, the Spark UI is excellent but does not
have immediate access for clusters for running on Dataproc. Fortunately, there is a [guide](https://cloud.google.com/dataproc/docs/concepts/cluster-web-interfaces) to set it up.
In short, run the following two commands noting that we add the master suffix for the cluster name
and that the executive path for Chrome depends on your OS

```gcloud compute --project "<project-name>" ssh --zone "<zone>" --ssh-flag="-D 1080" "<cluster-name>-m"```

```<path-to-chrome> --proxy-server="socks5://localhost:1080" --host-resolver-rules="MAP * 0.0.0.0 , EXCLUDE localhost" --user-data-dir=/tmp/ ```

In the Chrome browser that opens navigate to `http://master-host-name:8088` if on YARN or
`http://master-host-name:50070` if on HDFS. Move to the Streaming on the far right to see
granular metrics such as Input Rate and Processing Time.