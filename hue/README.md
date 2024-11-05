# HUE - Hadoop User Experience

This [initialization action](https://cloud.google.com/dataproc/init-actions)
installs the latest version of [Hue](http://gethue.com) on a master node within
a [Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster.

## Using this initialization action

**:warning: NOTICE:** See [best practices](/README.md#how-initialization-actions-are-used) of using initialization actions in production.

You can use this initialization action to create a new Dataproc cluster with Hue
installed:

1.  Use the `gcloud` command to create a new cluster with this initialization
    action.

    ```bash
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/hue/hue.sh
    ```

2.  Once the cluster has been created, Hue is configured to run on port `8888`
    on the master node in a Dataproc cluster. To connect to the Hue web
    interface, you will need to create an SSH tunnel and use a SOCKS 5 Proxy
    with your web browser as described in the
    [dataproc web interfaces](https://cloud.google.com/dataproc/cluster-web-interfaces)
    documentation. In the opened web browser go to 'localhost:8888' and you
    should see the Hue UI.

## Important notes

*   If you wish to use Oozie in Hue, it must be installed before
    running this initialization action e.g. put the [Oozie
    initialization action](../oozie/README.md) before this one in the
    --initialization-actions list argument to `gcloud dataproc
    clusters create`.
    
****************************************************************************

## Example: Hue \+ Hive (SQL UI) \- basic SQL queries

1.  Create warehouse bucket:
 
    ```
    export WAREHOUSE_BUCKET=<BUCKET NAME>
    gsutil mb -l ${REGION} gs://${WAREHOUSE_BUCKET}
    ```

2.  Prepare dataset: Copy the sample dataset to your warehouse bucket:  
    
    ```
    gsutil cp gs://hive-solution/part-00000.parquet \
    gs://${WAREHOUSE_BUCKET}/datasets/transactions/part-00000.parquet
    ```
     
3.  Create Dataproc cluster using [hue.sh](https://github.com/GoogleCloudDataproc/initialization-actions/blob/master/hue/hue.sh) init script 
    
    ```
    export CLUSTER_NAME=<CLUSTER_NAME>
    export REGION=<REGION>
    export INIT_BUCKET=<INIT_BUCKET_NAME>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
    --region ${REGION} \
    --initialization-actions gs://${INIT_BUCKET}/hue.sh \
    --properties "hive:hive.metastore.warehouse.dir=gs://${WAREHOUSE_BUCKET}/datasets"
    ```

4.  Configure Hue editor in the Dataproc cluster. SSH on master node and edit hue.ini as follows:  
   1. Under \[beeswax\] section configure [Apache Hive connector](https://docs.gethue.com/administrator/configuration/connectors/\#apache-hive) by uncommenting hive\_server\_host and hive\_server\_port  
   2. Under \[\[ interpreters \]\] section add following block  
          ```
          [[[Beeswax]]]
          name=Hive
          interface=hiveserver2
          ```
   3. Restart hue: \[[link to section](\#changes-made-to-hue.ini-are-not-effective-after-restart-using-systemctl)\]  
   4. Exit master node

5.  Connect to the Hue web interface: Create an SSH tunnel and use a SOCKS 5 Proxy with your web browser as described in the dataproc web interfaces documentation. In the opened web browser go to 'localhost:8888' and you should see the Hue UI.  
6.  Hue UI: Create an external Hive table for the dataset: 
    
    ```
    CREATE EXTERNAL TABLE transactions
    (SubmissionDate DATE, TransactionAmount DOUBLE, TransactionType STRING)
    STORED AS PARQUET
    LOCATION 'gs://${WAREHOUSE_BUCKET}/datasets/transactions';"
    ```
    ![External Hive table for the dataset](https://github.com/e55010104110/initialization-actions/blob/master/hue/create-hive-table.png)
    
7.  Run the following simple HiveQL query to select the 10 transactions:
   
    ```
    SELECT *
    	FROM transactions
    	LIMIT 10;"
    ```
    ![HiveQL query to select the 10 transactions](https://github.com/e55010104110/initialization-actions/blob/master/hue/simple-hiveql.png)
    
8.  Another query:

    ```
    SELECT TransactionType, COUNT(TransactionType) as Count
        FROM transactions
        WHERE SubmissionDate = '2017-08-22'
        GROUP BY TransactionType;"
    ```
   ![Another query](https://github.com/e55010104110/initialization-actions/blob/master/hue/another-query.png)
   
****************************************************************************

## Common issues

1.  ### Missing configurations for the distributed Hadoop services

![Hue UI](https://github.com/e55010104110/initialization-actions/blob/master/hue/hue-ui.png)

The hue.ini configuration file is configured to assume a single-node cluster with hostnames set to localhost and port numbers set to default values. For a distributed system, the different sections for each Hadoop service must be updated with the correct hostnames and ports for the servers on which the services are running. 

The hue.sh script performs some generic updates to the hostnames, such as replacing all occurrences of localhost with the fully qualified domain name (FQDN), but additional configurations may be required based on the available services, as described in the documentation for [How to configure Hue for your Hadoop cluster.](https://gethue.com/how-to-configure-hue-in-your-hadoop-cluster/)

2.  ### Changes made to hue.ini are not effective after restart using systemctl {#changes-made-to-hue.ini-are-not-effective-after-restart-using-systemctl}

    `systemctl restart hue.service` leaves behind some orphaned processes and the following command can be used to perform a clean restart of Hue: `sudo /etc/init.d/hue force-stop && sudo /etc/init.d/hue start`

3.  ### User \[hue\] not defined as proxyuser" when integrated with Oozie

    This is caused by missing ProxyUser/impersonation configuration for the logged in user in Oozie.   
    [Hue.sh](https://github.com/GoogleCloudDataproc/initialization-actions/blob/12792d9d40821e1fad202756e2532a1a8768fe54/hue/hue.sh\#L136) only adds the default user Hue as a ProxyUser, therefore all the other users must also be added. 

    1.  Identify the impacted user for example in the error message below it is the  doAs user \`hueuser\`:  *401 Client Error: Unauthorized for url: http://\<redacted\>:11000/oozie/v1/jobs?len=100\&doAs=hueuser\&filter=user%3Dhueadmin%3Bstartcreatedtime%3D-7d\&user.name=hue\&offset=1\&timezone=America%2FLos\_Angeles\&jobtype=wf {"errorMessage":"User \[hue\] not defined as proxyuser","httpStatusCode":401} (error 401\)*  

    2.  Add the following properties to oozie-site as follows replacing the \#USER\# with the impacted user in step 1 above. 

    ```
    <property>
        <name>oozie.service.ProxyUserService.proxyuser.\#USER\#.hosts\</name>  
        <value>*</value>  
    </property>  
    <property>  
        <name>oozie.service.ProxyUserService.proxyuser.\#USER\#.groups\</name\> 
        <value>*</value>  
    </property>
    ```

    3.  Restart Oozie

        1.   Identify oozie process PID   
        2.   Stop the Oozie process with `sudo systemctl stop oozie.service` and if it doesn't stop successfully, then use `kill -9 <oozie-process-PID>` to kill the process   
        3.   Confirm that the process is not running:  `ps \-ef | grep oozie`  
        4.   Restart Oozie service `sudo systemctl restart oozie.service`
   
****************************************************************************
