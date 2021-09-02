# Dataproc Metastore gRPC proxy

If you have a Dataproc Metastore service that supports gRPC, you can use this
initialization action to install a Thrift-gRPC proxy that allows client
components in the Dataproc cluster to communicate with the metastore.

## Using this initialization action

**:warning: NOTICE:** See
[best practices](/README.md#how-initialization-actions-are-used) of using
initialization actions in production.

Your Dataproc Metastore service must use Hive version 2.3.6 or 3.1.2, and your
Dataproc cluster must use image version 1.5 or later to use this initialization
action.

1.  Using the `gcloud` command to create a new cluster with this initialization
    action.

    ```bash
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    HIVE_VERSION=<hive_version>  # The Hive version used by the Dataproc Metastore service. Use 2.3.6 or 3.1.2
    # Use image version 1.5-debian10 for Hive Metastore version 2.3.6 and
    # 2.0-debian10 for Hive Metastore version 3.1.2
    IMAGE_VERSION=<image_version>
    METASTORE_ID=<metastore_id>
    WAREHOUSE_DIR=$(gcloud beta metastore services describe "${METASTORE_ID}" --location "${REGION}" --format="get(hiveMetastoreConfig.configOverrides[hive.metastore.warehouse.dir])")
    METASTORE_URI=$(gcloud beta metastore services describe "${METASTORE_ID}" --location "${REGION}" --format="get(endpointUri)")
    gcloud dataproc clusters create ${CLUSTER_NAME} \
    --region ${REGION} \
    --scopes "https://www.googleapis.com/auth/cloud-platform" \
    --optional-components=DOCKER \
    --image-version ${IMAGE_VERSION} \
    --metadata "proxy-uri=${METASTORE_URI},hive-version=${HIVE_VERSION}" \
    --properties "hive:hive.metastore.uris=thrift://localhost:9083,hive:hive.metastore.warehouse.dir=${WAREHOUSE_DIR}" \
    --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/metastore-grpc-proxy/metastore-grpc-proxy.sh
    ```

1.  Once the cluster is created, the gRPC proxy will listen for Thrift
    connections on localhost:9083 on all VMs in the cluster.


## Important notes

*   Personal auth Dataproc clusters cannot use this initialization action.
*   You cannot enable Kerberos in your Dataproc cluster if you use this
    initialization action.
