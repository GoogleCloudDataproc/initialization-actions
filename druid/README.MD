# Druid

This script installs Druid on dataproc clusters. To learn more about Druid see documentation: http://druid.io/docs/latest/design/.


## Using this initialization action
You can use this initialization action to create a new Dataproc cluster with Druid installed.

Once the cluster has been created, Druid is configured to run on port `8090`.

Example of gcloud command for Druid initialization on standard cluster:

```
gcloud dataproc clusters create druid --master-machine-type n1-standard-4 --num-workers 2 --worker-machine-type n1-standard-4 --image-version 1.3-deb9 --initialization-actions 'gs://dataproc-initialization-actions/zookeeper/zookeeper.sh','gs://dataproc-initialization-actions/druid/druid.sh'
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

This script is used for testing dr-elephant on 1.1,1.2,1.3 dataproc images with single,
standard and HA configurations. After clusters are created, you can submit batch task on them.

Remember about setting up zookeeper init action first if you decide to use Druid upon Standard configuration.

Dr. Elephant UI can be accessed after connection with command:
```
gcloud compute ssh clusterName-m* -- -L 8090:clusterName-m*:8090
```

In ```../druid-0.12.3``` directory exists folder named examples. Inside you can find sample tasks that can be used for setup validation.

Within ```../druid-0.12.3``` directory fire: 
```curl -L -X 'POST' -H 'Content-Type:application/json' -d @examples/wikipedia-index.json http://localhost:8090/druid/indexer/v1/task``` and then it should be task id returned.
This task can be found on UI after a moment and further it should be displayed SUCCESS status if config is ok.

Remember to call ```-L``` option - it is crucial because of redirection between service on cluster nodes.

Then just open a browser and type ```localhost:8090``` address. Your task statistics should be there.

## Automated tests

You can verify setup using automated script ```test_druid.py```.
