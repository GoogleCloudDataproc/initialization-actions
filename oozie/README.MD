# Oozie 

This initialization action installs the [Oozie](http://oozie.apache.org) workflow scheduler on a [Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster. The Oozie server, client, and web interface are installed. 

## Using this initialization action
You can use this initialization action to create a new Dataproc cluster with Oozie installed by:

1. Uploading a copy of this initialization action (`oozie.sh`) to [Google Cloud Storage](https://cloud.google.com/storage).
2. Using the `gcloud` command to create a new cluster with this initialization action. The following command will create a new cluster named `<CLUSTER-NAME>`, specify the initialization action stored in `<GCS-BUCKET>`:
   
    ```bash
    gcloud dataproc clusters create <CLUSTER-NAME> \
    --initialization-actions gs://<GCS-BUCKET>/oozie.sh
    ```
3. Once the cluster has been created Oozie should be running on the master node. 

You can find more information about using initialization actions with Dataproc in the [Dataproc documentation](https://cloud.google.com/dataproc/init-actions).

## Testing Oozie
You can test this Oozie installation by running the `oozie-examples` included with Oozie. The examples are in an archive at `/usr/share/doc/oozie/oozie-examples.tar.gz`. To run the MapReduce example, you can do the following:

1. Move the examples to your home directory:<br/>
`cp /usr/share/doc/oozie/oozie-examples.tar.gz ~`
2. Decompress the archive:<br/>
`tar -zxvf oozie-examples.tar.gz`
3. Edit the MapReduce example with details for your cluster:<br/>
`nameNode=hdfs://<cluster-name-m>:8020`<br/>
`jobTracker=<cluster-name-m>:8032`
4. Move the Oozie examples to HDFS:<br/>
`hadoop fs -put ~/examples/ /user/<username>/`
5. Run the example on the command line with:<br/>
`oozie job -oozie http://127.0.0.1:11000/oozie -config \
~/examples/apps/map-reduce/job.properties -run`
   

## Oozie web interface
The Oozie web interface is available on port `11000` on the master node of the cluster. For example, the Oozie web intarface would be available at the following address for a cluster named `my-dataproc-cluster`:

    http://my-dataproc-cluster-m:11000/oozie
    
To connect to the web interface you will need to create an SSH tunnel and use a SOCKS proxy. Instructions on how to do this are available in the [cloud dataproc documentation](https://cloud.google.com/dataproc/cluster-web-interfaces).

## Important notes
* As Oozie is updated in BigTop the version of Oozie which is installed with this action will change.
* HDFS is running on port `8020` and the (YARN) JobTracker is on port `8032` which may be useful information for some jobs.
