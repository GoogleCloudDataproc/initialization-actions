
# Hive LLAP Initialization Action
This initialization action configures Google Cloud Dataproc to run Hive LLAP. For more information on Hive LLAP please visit: https://cwiki.apache.org/confluence/display/Hive/LLAP

## Optional Configuration Options
* num_llap_nodes: The number of nodes you wish LLAP to run on. This configuration will use 100% of the YARN resources LLAP is deployed on. Do not deploy on 100% of the deployed dataproc workers. You need available nodes in order to run the Tez Application Masters spawned by Hive sessions. Understand how many concurrent queries you'd like to support to determine out how many workers you need available to support your users. Please visit: https://community.cloudera.com/t5/Community-Articles/LLAP-sizing-and-setup/ta-p/247425 for more details on tuning LLAP.
* ssd: LLAP can extend the memory cache pool to local SSD on the workers it's deployed on. This increases the amount of data that can be cached to help with accelerating queries. This option may only be used when you include local ssd (num-worker-local-ssds flag) when deploying your dataproc cluster. Today we only support 1 SSD deployment. Note that if you use this option, you cannot start/stop your dataproc instance. 
* exec_size_mb: This is the size of the executors running on LLAP. This is related to the hive.tez.container.size hive parameter. By default this number is 4096MB if not supplied directly. This knob allows you to configure clusters for uses cases where the default exec size may not be appropriate. 
* init-actions-repo: if you are sourcing the initialization action script from a personal bucket, this metadata configuration tells the script where to find the startup script for the LLAP service. This configuration is not required. If you don't specific the init-actions-repo, the required files will be procured through the default initialization action bucket.

## Using this initialization action

**:warning: NOTICE:** See [best practices](/README.md#how-initialization-actions-are-used) of using initialization actions in production.

You can use this initialization action to create a new Dataproc cluster with LLAP enabled. When this initialization action runs, it will automatically download an additional script, start_llap.sh. This script is used to auto-start LLAP and can be subsequently used to re-start LLAP if the cluster is stopped or shutdown. This script will reside on the the master node (master node 0 on HA deployments) on /usr/lib/hive-llap. If a user wants to download the initialization action from a seperate repo instead of the please specify the cloud storage bucket by adding the metadata parameter, init-actions-repo as written about above in the Optional Configuration Options section. Note that the initialization action looks for the script in /hive-llap/start_llap.sh so ensure that your personal bucket adhers to this path. Otherwise the script will automatically download from the initializations bucket by default. 

1. Use the `gcloud` command to create a new cluster with this initialization action. By default, LLAP will consume all nodes but one unless you specifiy the number of nodes to consume with the num-llap-nodes metadata parameter. You need at least 1 node open to run Tez AM's. 
   ```bash
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --optional-components ZOOKEEPER \
        --image-version 2.0-debian10 \
        --metadata num-llap-nodes=1 \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/hive-llap/llap.sh
    ```

2. Use the `gcloud` command to create a new cluster with this initialization action with SSD's configured and the size of the LLAP exeuctors defined.

   ```bash
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --optional-components ZOOKEEPER \
        --image-version 2.0-debian10 \
        --num-worker-local-ssds 1 \
        --metadata ssd=true,num-llap-nodes=1,exec_size_mb=3000 \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/hive-llap/llap.sh
    ```

3. Use the `gcloud` command to create a new cluster with this initialization action with SSD's configured and the size of the LLAP exeuctors defined and sourcing the initialization action files from a personal cloud bucket. 

```bash
    REGION=<region>
    BUCKET=<your_init_actions_bucket>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --optional-components ZOOKEEPER \
        --image-version 2.0-debian10 \
        --num-worker-local-ssds 1 \
        --metadata ssd=true,num-llap-nodes=1,exec_size_mb=3000,init-actions-repo=gs://${BUCKET}
        --initialization-actions gs://${BUCKET}/hive-llap/llap.sh
```

You can find more information about using initialization actions with Dataproc in the [Dataproc documentation](https://cloud.google.com/dataproc/init-actions).

## Important notes

* This initialization action will only work with Debian and Ubuntu Dataproc 2.0+ images. 
* Clusters must be deployed with the zookeeper optional component selected
* This initialization action doesn't support single node deployments
* This initialization action supports HA and non-HA depolyments
* It is highly recommended to deploy high memory machine types to ensure LLAP will have space available for cache
* LLAP will auto configure based on the machine shape. It will adhere to 4GB/executor and ensure that the LLAP Container itself has enough headroom for the JVM (6% or 6GB MAX). Any remainder is treated as off heap cache
* Clusters must have at least 2 worker nodes to deploy LLAP. Set num-llap-nodes=[num llap nodes] to tell the script how many LLAP instances to run. If the number of LLAP instances are >= the worker node count, the provisioning will fail. LLAP runs on 100% of the resources of a yarn node. 
* LLAP enables extending the cache pool to include SSD's. Users can deploy dataproc workers with local SSD's to extend LLAP's cache pool. To enable the SSD configuration, simply deploy dataproc with 1 local SSD and apply custom cluster metadata SSD=1 to trigger the configuration of the SSD in LLAP cache. 
* Only 1 Hive Server is deployed. hiveserver2-interactive is the zookeeper namespace for HA deployments.
* Hive has been configured to support ACID transactions with this deployment. 
