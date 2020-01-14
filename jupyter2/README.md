# Jupyter Notebook

This initialization action installs the latest version of [Jupyter Notebook](http://jupyter.org/) with `pip` and Python 2. Conda and Python 3 users should instead use the original [Jupyter init action](https://github.com/GoogleCloudPlatform/dataproc-initialization-actions/tree/master/jupyter).

__Use the Dataproc [Jupyter Optional Component](https://cloud.google.com/dataproc/docs/concepts/components/jupyter)__. Clusters created with Cloud Dataproc image version 1.3 and later can install Jupyter Notebook without using this initialization action. The Jupyter Optional Component's web interface can be accessed via [Component Gateway](https://cloud.google.com/dataproc/docs/concepts/accessing/dataproc-gateways) without using SSH tunnels.

## Using this initialization action

**:warning: NOTICE:** See [best practices](/README.md#how-initialization-actions-are-used) of using initialization actions in production.

Usage is similar to the original `jupyter` init action.

```
REGION=<region>
CLUSTER_NAME=<cluster_name>
gcloud dataproc clusters create ${CLUSTER_NAME} \
    --region ${REGION} \
    --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/jupyter2/jupyter2.sh
```

### Options

A few of same options are supported here:

* `--bucket=gs://<some-bucket>` (the cluster staging bucket) is used for storing and retrieving notebooks. Set this to the same value when recreating clusters to share notebooks between them. By default, clusters in your project in the same region use the same bucket.
* `--metadata JUPYTER_PORT=<some-port>` can be used to override the default port (8123)
* `--metadata JUPYTER_AUTH_TOKEN=<some-string>` can be used to secure the notebook and ensure only users with the token can access it. See [Securing Jupyter](http://jupyter-notebook.readthedocs.io/en/stable/security.html) for more information.

For example:

```
REGION=<region>
CLUSTER_NAME=<cluster_name>
gcloud dataproc clusters create ${CLUSTER_NAME} \
    --region ${REGION} \
    --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/jupyter2/jupyter2.sh \
    --bucket gs://mybucket \
    --metadata JUPYTER_PORT=80,JUPYTER_AUTH_TOKEN=mytoken
```

