# TonY - TensorFlow over YARN

This [initialization action](https://cloud.google.com/dataproc/init-actions) installs the latest version of [TonY](https://github.com/linkedin/TonY)
on a master node within a [Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster.

## Using this initialization action

You can use this initialization action to create a new Dataproc cluster with TonY installed:

1. Use the `gcloud` command to create a new cluster with this initialization action.  The following command will create a new cluster named `<CLUSTER_NAME>`.

    ```bash
    gcloud dataproc clusters create <CLUSTER_NAME> \
      --initialization-actions gs://dataproc-initialization-actions/tony/tony.sh
    ```

    You can also pass specific metadata:
    
    ```bash
    gcloud dataproc clusters create <CLUSTER_NAME> \
      --initialization-actions gs://dataproc-initialization-actions/tony/tony.sh \
      --metadata name1=value1,name2=value2... 
    ```
    
    Supported metadata parameters:
    
     - num_workers: 
     - worker_memory
     - ps_memory
        
    These parameters are defined here: TonY [configurations](https://github.com/linkedin/TonY/wiki/TonY-Configurations)
    
    Example:
    
    ```bash
    gcloud dataproc clusters create <CLUSTER_NAME> \
      --initialization-actions gs://dataproc-initialization-actions/tony/tony.sh \
      --metadata num_gpus=0,num_workers=4,worker_memory=4g 
    ```
    
2. Once the cluster has been created, you can access the Hadoop web interface on the master node in a Dataproc cluster. To connect to the Hadoop web interface, you will need to create an SSH tunnel and use a SOCKS 5 Proxy with your web browser as described in the [dataproc web interfaces](https://cloud.google.com/dataproc/cluster-web-interfaces) documentation. In the opened web browser go to 'localhost:8088' and you should see the TonY UI.

3. TonY installation is located by default in the following folder:

    ```bash
    /usr/local/src/TonY
    ```
    
For more information take a look at the TonY in GCP [installation guide]()