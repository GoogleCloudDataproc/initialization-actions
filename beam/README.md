# Beam Initialization Actions

While Beam is primarily a SDK, jobs running under its portability framework
require a job service to run correctly.  This directory contains a setup script
to setup a properly configured beam job services.

## Using This Initialization Action

Use the `gcloud` command to create a new cluster with beam, flink, and docker
enabled:

```bash
gcloud dataproc clusters create <CLUSTER_NAME> \
--initialization-actions gs://dataproc-initialization-actions/flink/flink.sh,gs://dataproc-initialization-actions/docker/docker.sh,gs://dataproc-initialization-actions/beam/beam_job_service.sh \
--image-version=1.2
--metadata=flink-snapshot-url=https://archive.apache.org/dist/flink/flink-1.5.3/flink-1.5.3-bin-hadoop28-scala_2.11.tgz
```

Beam 2.7 requires Flink 1.5, which is not the default Flink version in Dataproc
1.2.  Therefore users must specify a custom flink snapshot, as above.

This will set up a Beam Job Service on port `8099` of the master node. You can 
submit portable beam jobs against this port as normal. For example to run the Go 
Wordcount example from the master node, one could upload the wordcount binary
and then run:

```bash
./wordcount \
  --runner flink \
  --endpoint localhost:8099 \
  --experiments beam_fn_api \
  --output=<out> \
  --container_image <image_repo>/go:v2.7.0
```

The Beam Job Service port will need to be opened if users wish to submit beam
jobs from machines outside the cluster.  Please refer to the
[guide](https://cloud.google.com/vpc/docs/using-firewalls) for instructions on
how to accomplish this.

## Metadata Variables

The Beam init action uses the following metadata variables

| Metadata Key | Default | Description |
| --- | --- |
| beam-artifacts-dir | [cluster staging bucket](https://cloud.google.com/dataproc/docs/guides/create-cluster#auto-created_staging_bucket) | A writeable GCS bucket that Beam can use to stage artifacts |
| beam-job-service-snapshot | [snapshot](http://repo1.maven.org/maven2/org/apache/beam/beam-runners-flink_2.11-job-server/2.6.0/beam-runners-flink_2.11-job-server-2.6.0.jar) | The URL of a Beam job service snapshot |
| beam-image-enable-pull | false | When set to true, the init action will attempt to pull beam worker images for efficient access later |
| beam-image-version | v2.7.0 | The image version to use when selecting a tagged image |
| beam-image-repository | apache.bintray.io/beam | The image repository root to pull images from. As of this writing (September 5th, 2018) these images have not been published yet.  Therefore it is recommended that users build and store their own images when using this init action. |

