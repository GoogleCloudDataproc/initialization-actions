
# Hive LLAP Initialization Action
This initial action configures Google Cloud Dataproc to run Hive LLAP. For more information on Hive LLAP please visit: https://cwiki.apache.org/confluence/display/Hive/LLAP

## Using this initialization action

**:warning: NOTICE:** See [best practices](/README.md#how-initialization-actions-are-used) of using initialization actions in production.

You can use this initialization action to create a new Dataproc cluster with LLAP enabled:

1. Use the `gcloud` command to create a new cluster with this initialization action. By default, LLAP will consume all nodes but one. If you want to customize the number
of nodes LLAP runs on, please use the metadata paramater, num-llap-nodes to specifiy how many nodes to deploy LLAP on. You need at least 1 node open to run Tez AM's. 

   ```bash
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --optional-components ZOOKEEPER \
        --image-version 2.0-debian10 \
        --metadata num-llap-nodes=1 \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/llap/llap.sh
    ```

2. Use the `gcloud` command to create a new cluster with this initialization action with SSD's configured.

   ```bash
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --optional-components ZOOKEEPER \
        --image-version 2.0-debian10 \
        --num-worker-local-ssds 1 \
        --metadata ssd=true,num-llap-nodes=1 \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/llap/llap.sh
    ```

3. You can test your LLAP setup by creating an external table 

Download a file from the citibike for testing. Unzip file and place on HDFS

```
wget https://s3.amazonaws.com/tripdata/202008-citibike-tripdata.csv.zip
unzip 202008-citibike-tripdata.csv.zip
hdfs dfs -put 202008-citibike-tripdata.csv /tmp
```

Use beeline to connect to hive to run test queries on the downloaded data. These tests will demonstrate simple queries on data with LLAP as well as issuing ACID transactions on the data. 


set hive.tez.exec.print.summary=true  will provide statistics on the effectiveness of LLAP as you run queries on the data. This is important when looking at metrics like cache hit rate. 

```
beeline -u "jdbc:hive2://[master node]:10000"

set hive.tez.exec.print.summary=true;

create table citibike (
trip_duration double,
start_time timestamp,
stop_time timestamp,
start_station_id integer,
start_station_name string,
start_station_latitude string,
start_station_longitude string,
end_station_id integer,
end_station_name string,
end_station_latitude string,
end_station_longitude string,
bike_id integer,
user_type string,
birth_year string,
gender string)
ROW FORMAT DELIMITED
   FIELDS TERMINATED BY ','
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1"); 

load data inpath '/tmp/202008-citibike-tripdata.csv' overwrite into table citibike;

select count(*),start_station_name from citibike group by start_station_name;

create table citibike_orc (
trip_duration double,
start_time timestamp,
stop_time timestamp,
start_station_id integer,
start_station_name string,
start_station_latitude string,
start_station_longitude string,
end_station_id integer,
end_station_name string,
end_station_latitude string,
end_station_longitude string,
bike_id integer,
user_type string,
birth_year string,
gender string)
STORED AS ORC
tblproperties("transactional"="true"); 

insert overwrite table citibike_orc select * from citibike;

update citibike_orc set start_station_name="foo" where start_station_id=3735;

select count(*),start_station_name from citibike_orc where start_station_name="foo" group by start_station_name;

select count(*),start_station_id from citibike_orc group by start_station_id;
```

You can find more information about using initialization actions with Dataproc in the [Dataproc documentation](https://cloud.google.com/dataproc/init-actions).

## Important notes

* This initialization action will only work with Debian Dataproc 2.x + images. 
* Clusters must be deployed with the zookeeper optional component selected
* This initialization action doesn't currently support Kerberos
* This initialization action supports HA and non-HA depolyments. 
* It is highly recommended to deploy high memory machine types to ensure LLAP will have space available for cache
* LLAP will auto configure based on the machine shape. It will adhere to 4GB/executor and ensure that the LLAP Container itself has enough headroom for the JVM (6% or 6GB MAX). Any remainder is treated as off heap cache
* Clusters must have at least 2 worker nodes to deploy LLAP. Set num-llap-nodes=[num llap nodes] to tell the script how many LLAP instances to run. If the number of LLAP instances are >= the worker node count, the provisioning will fail. 
* LLAP enables extending the cache pool to include SSD's. Users can deploy dataproc workers with local SSD's to extend LLAP's cache pool. To enable the SSD configuration, simply deploy dataproc with 1 local SSD and apply custom cluster metadata SSD=1 to trigger the configuration of the SSD in LLAP cache. 
* Only 1 Hive Server is deployed. hiveserver2-interactive is the zookeeper namespace for HA deployments.
* Hive has been configured to support ACID transactions with this deployment. 
