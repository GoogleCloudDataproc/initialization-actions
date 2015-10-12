# Dataproc Initialization Actions

When creating a [Google Cloud Dataproc](https://cloud.google.com/dataproc/) cluster, you can specify [initialization actions](https://cloud.google.com/dataproc/init-actions) in executables and/or scripts that Cloud Dataproc will run on all nodes in your Cloud Dataproc cluster immediately after the cluster is set up.

## How are initialization actions used?
Initialization actions are stored in a [Google Cloud Storage](https://cloud.google.com/storage) bucket and can be passed as a paramater to the `gcloud` command or the `clusters.create` API when creating a Dataproc cluster. For example, to specify an initialization action when creating a cluster with the `gcloud` command, you can run:

    gcloud beta dataproc clusters create CLUSTER-NAME
    [--initialization-actions [GCS_URI,...]]
    [--initialization-action-timeout TIMEOUT]

## Why these samples are provided
These samples are provided to show how various packages and components can be installed on Cloud Dataproc clusters. You should understand how these samples work before running them on your clusters. The initialization actions provided in this repository are provided **without support** and you **use them at your own risk**.

## For more information
For more information, review the [Dataproc documentation](https://cloud.google.com/dataproc/init-actions). You can also pose questions to the [Stack Overflow](http://www.stackoverflow.com) comminity with the tag `google-cloud-dataproc`.
See our other [Google Cloud Platform github
repos](https://github.com/GoogleCloudPlatform) for sample applications and
scaffolding for other frameworks and use cases.


## Contributing changes

* See [CONTRIBUTING.md](CONTRIBUTING.md)

## Licensing

* See [LICENSE](LICENSE)
