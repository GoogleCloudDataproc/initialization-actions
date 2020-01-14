# TonY - TensorFlow on YARN

This [initialization action](https://cloud.google.com/dataproc/init-actions) installs the latest version of [TonY](https://github.com/linkedin/TonY)
on a master node within a [Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster.

## Using this initialization action

**:warning: NOTICE:** See [best practices](/README.md#how-initialization-actions-are-used) of using initialization actions in production.

You can use this initialization action to create a new Dataproc cluster with TonY installed:

1. Use the `gcloud` command to create a new cluster with this initialization action.

    ```bash
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/tony/tony.sh
    ```

    You can also pass specific metadata:
    
    ```bash
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/tony/tony.sh \
        --metadata name1=value1,name2=value2... 
    ```
    
    Supported metadata parameters:
    
     - worker_instances 
     - worker_memory
     - ps_instances
     - ps_memory
     - tensorflow version
     - pytorch version
     - torch vision version
     - tf_gpu
        
    These parameters are defined here: TonY [configurations](https://github.com/linkedin/TonY/wiki/TonY-Configurations)
    
    Example:
    
    ```bash
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/tony/tony.sh \
        --metadata worker_instances=4,worker_memory=4g,ps_instances=1,ps_memory=2g
    ```
    
    **Note:** For settings not defined in this configuration, you can pass a separate configuration when launching tasks
    or contribute to this repository and add support for more TonY configurations.
    
2. Once the cluster has been created, you can access the Hadoop web interface on the master node in a Dataproc cluster. To connect to the Hadoop web interface, you will need to create an SSH tunnel and use a SOCKS 5 Proxy with your web browser as described in the [dataproc web interfaces](https://cloud.google.com/dataproc/cluster-web-interfaces) documentation. In the opened web browser go to 'localhost:8088' and you should see the TonY UI.

3. TonY installation is located by default in the following folder:

    ```bash
    /opt/tony/TonY
    ```
    
4. TonY examples is located in the following folder:

    ```bash
    /opt/tony/TonY-samples
    ```
    By default we install two examples:
    
    - TensorFlow
    - PyTorch
    
For more information and to run some TonY examples, take a look at [TonY examples](https://github.com/linkedin/TonY/tree/master/tony-examples)
A working example can be found on validate.sh

## Testing
You can easily test TonY is running by using ```validate.sh ${cluster_prefix} ${bucket_name}``` script.

* ```cluster_prefix``` argument sets a prefix in created cluster name
* ```bucket_name``` argument sets a place where init action will be placed

This script is used for testing TonY using 1.3 Dataproc images with standard configurations. 
After clusters are created, script submits Hadoop jobs on them.


## Important notes

* This script will install TonY in the master node only
* Virtual environments are installed for both TensorFlow and PyTorch examples.
* TonY is supported with STANDARD configuration (1 master/2+ workers) in Dataproc 1.3.
* TonY supports GPU using Hadoop 3.1 (YARN-6223). Currently not supported with Dataproc