# Druid

This script installs Druid on dataproc clusters. To learn more about Druid see documentation: http://druid.io/docs/latest/design/.


## Using this initialization action
You can use this initialization action to create a new Dataproc cluster with Druid installed.

Once the cluster has been created, Druid Overlord Console is configured to run on port `8090`. You can change this port with additional parameter:
``
--metadata=druid-overlord-port=xxxx
``
Druid Coordinator Console is configured to run on port `8081`. You can change this port with additional parameter:
``
--metadata=druid-coordinator-port=xxxx
``

By default Druid initialized with this script is configured to use hdfs as a segment storage. It is possible to make this running with GCS bucket by adding metadata with initialization command. For example: 
```
--metadata=gcs-bucket=YOUR_BUCKET
```

Example of gcloud command for Druid initialization on standard cluster:

```
gcloud dataproc clusters create $CLUSTER_NAME --initialization-actions 'gs://dataproc-initialization-actions/zookeeper/zookeeper.sh','gs://dataproc-initialization-actions/druid/druid.sh'
```

Druid can be used with Cloud SQL Proxy (init action inside repo). To make Druid running with this please follow steps:
  1. Create SQL instance with command: 
  ```gcloud sql instances create yourname-db --region us-central1```
  2. Define metadata to your create cluster command: 
  ```--metadata=hive-metastore-instance=your-project-name:us-central1:yourname-db```
  3. Define scope to your create cluster command:
  ```--scopes sql-admin```
  4. Add to your gcloud dataproc clusters create command metadata and scope
  5. Put Cloud SQL init action at the first position on initialization actions list:
  6. Combine command with additional data and execute - example below:
  ```gcloud dataproc clusters create druid --num-masters 3 --num-workers 2 --scopes sql-admin --metadata=hive-metastore-instance=your-project-name:us-central1:yourname-db --image-version=1.0 --initialization-actions 'gs://dataproc-initialization-actions/cloud-sql-proxy/cloud-sql-proxy.sh','gs://dataproc-initialization-actions/druid/druid.sh' --format=json --initialization-action-timeout 15m ```

## Testing

This script is used for testing Druid on 1.1, 1.2, 1.3 dataproc images with single,
standard and HA configurations. After clusters are created, you can submit batch task on them.

Remember about setting up zookeeper init action first if you decide to use Druid upon Standard configuration.

Druid UI can be accessed after connection with command:
```
gcloud compute ssh clusterName-m* -- -L 8090:clusterName-m*:8090
```

In ```/opt/druid/apache-druid-0.13.0-incubating/quickstart/tutorial``` directory you can find sample tasks that can be used for setup validation.

You can verify correctness of your setup with the following command:
```/opt/druid/apache-druid-0.13.0-incubating/bin/post-index-task --file /opt/druid/apache-druid-0.13.0-incubating/quickstart/tutorial/wikipedia-index.json```

Once task is finished, the detailed info should be displayed under druid-coordinator web console (UI). 

## Automated tests

You can verify setup using automated script ```test_druid.py```. In order to run tests fire ```python3 -m unittest druid.test_druid```.
Please be aware that automated test is not compliant with other port than 8090 and validation may fail.
