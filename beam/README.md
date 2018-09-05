# Beam Initialization Actions

While Beam is primarily a SDK, jobs running under its portability framework
require a job service to run correctly.  This directory contains a setup script
to setup a properly configured beam job services.

**WARNING:** The Beam portability framework is **under active development** and
should not be used in a production context.

Due to the current development status of Beam's portability framework, users are
responsible for building and maintaining their own Beam artifacts manually.
Instructions are included below.  The current development status of the Beam
portability framework can be found
[here](https://beam.apache.org/contribute/portability/#status).

## Building Beam Artifacts

There are two categories of artifacts that will need to be generated for this
initialization action:

| Job Service | This microservice runs on the master node, accepting new beam jobs on port 8099.  It is configured to submit Beam jobs to Flink. |
| Worker Container Images | These docker images are what run language-specific code on worker nodes. |

Throughout the build process, we will refer to the following terms, which will
need to be substituted into build commands if building manually.  In bash, one
convenient way to use them is to set environment variables using `export
<term>=<value>`.

| `BEAM_JOB_SERVICE_DESTINATION` | A GCS directory path accessible by both the build machine and the cluster to be created. |
| `BEAM_CONTAINER_IMAGE_DESTINATION` | A Docker repository path prefix accessible by both the build machine and the cluster to be created. |
| `BEAM_SOURCE_VERSION` | A tag, branch, or commit hash in the Beam source repositories to build artifacts from. (default: `master`) |

### Automated Build

The `util` directory contains a script to assist in building Beam artifacts.
From this directory on a build machine, you can invoke it with:

```bash
bash ./util/build-beam-artifacts.sh <BEAM_JOB_SERVICE_DESTINATION> <BEAM_CONTAINER_IMAGE_DESTINATION> [<BEAM_SOURCE_VERSION>]
```

### Manual Build

Alternatively, developers can build the necessary artifacts manually.  To get
started, first clone the beam source code into a working directory.

```bash
git clone https://github.com/apache/beam.git
cd beam
git checkout ${BEAM_SOURCE_VERSION}
```

#### Build the Job Service

This step will build a standalone job service jar, and upload it to that path
for use during cluster creation.

```bash
./gradlew :beam-runners-flink_2.11-job-server:shadowJar
```

After the jar has been built, it will need to be uploaded to a GCS path that
clusters can access during initialization.

```bash
gsutil cp \
  ./runners/flink/job-server/build/libs/beam-runners-flink_2.11-job-server-*-SNAPSHOT.jar \
  <BEAM_JOB_SERVICE_DESTINATION>/beam-runners-flink_2.11-job-server-latest-SNAPSHOT.jar
```

#### Build the Worker Container Images

This step will build the docker images used by Beam worker tasks.

```bash
./gradlew docker
```

This will build a number of docker images with names that look like
`<USER>-docker-apache.bintray.io/beam/<LANGUAGE>`.  These need to be renamed and
pushed to a docker repository path that clusters can access during
initialization.  We recommend tagging these images with the
`BEAM_SOURCE_VERSION` they were generated from.

```bash
docker tag \
  <USER>-docker-apache.bintray.io/beam/<LANGUAGE> \
  <BEAM_CONTAINER_IMAGE_DESTINATION>/<LANGUAGE>:<BEAM_SOURCE_VERSION>
docker push <BEAM_CONTAINER_IMAGE_DESTINATION>/<LANGUAGE>:<BEAM_SOURCE_VERSION>
```

## Creating a Cluster

Running a Beam cluster currently requires the following initialization actions:

  - `docker/docker.sh`
  - `flink/flink.sh`
  - `beam/beam_job_service.sh`

In addition, a number of metadata variables need to be set correctly.

| Metadata Key | Value |
| --- | --- |
| beam-job-service-snapshot | The GCS path of your JobService jar (see above) |
| beam-image-enable-pull | `true` |
| beam-image-repository | `<BEAM_CONTAINER_IMAGE_DESTINATION>` (see above) |
| beam-image-version | the tag used for container images (default: `latest`) |
| flink-start-yarn-session | `true` |
| flink-snapshot-url | URL to a flink 1.5 snapshot such as [this one](https://archive.apache.org/dist/flink/flink-1.5.3/flink-1.5.3-bin-hadoop28-scala_2.11.tgz) |

Beam 2.7 requires Flink 1.5, which is not the default Flink version in Dataproc
1.2.  Therefore users must specify a custom flink snapshot.

In practice, the best way to handle this many metadata values is through a
script such as this one:

TODO: anonymize this
```bash
CLUSTER_NAME="$1
INIT_BUCKET="gs://dataproc-initialization-actions"
INIT_ACTIONS="${INIT_BUCKET}/docker/docker.sh"
INIT_ACTIONS+=",${INIT_BUCKET}/flink/flink.sh"
INIT_ACTIONS+=",${INIT_BUCKET}/beam/beam_job_service.sh"
FLINK_SNAPSHOT="https://archive.apache.org/dist/flink/flink-1.5.3/flink-1.5.3-bin-hadoop28-scala_2.11.tgz"
METADATA="beam-job-service-snapshot=<...>"
METADATA+=",beam-image-enable-pull=true"
METADATA+=",beam-image-repository=<...>"
METADATA+=",beam-image-version=latest"
METADATA+=",flink-start-yarn-session=true"
METADATA+=",flink-snapshot-url=${FLINK_SNAPSHOT}"

gcloud dataproc clusters create "${CLUSTER_NAME}" \
  --initialization-actions="${INIT_ACTIONS}" \
  --image-version="1.2" \
  --metadata="${METADATA}"
```

This will set up a Beam Job Service on port `8099` of the master node. You can 
submit portable beam jobs against this port as normal. For example to run the Go 
Wordcount example from the master node, one could upload the wordcount job
binary and then run:

```bash
./wordcount \
  --runner flink \
  --endpoint localhost:8099 \
  --experiments beam_fn_api \
  --output=<out> \
  --container_image <BEAM_CONTAINER_DESTINATION>/go:<BEAM_SOURCE_VERSION>
```

The Beam Job Service port will need to be opened if users wish to submit beam
jobs from machines outside the cluster.  Please refer to the
[guide](https://cloud.google.com/vpc/docs/using-firewalls) for instructions on
how to accomplish this.

## Metadata Variables

The Beam init action uses the following metadata variables.  It is **highly
recommended** that users build Beam artifacts as described above, and specify
them explicitly by setting these metadata keys to custom values.

| Metadata Key | Default | Description |
| --- | --- |
| beam-job-service-snapshot | [v2.6.0](http://repo1.maven.org/maven2/org/apache/beam/beam-runners-flink_2.11-job-server/2.6.0/beam-runners-flink_2.11-job-server-2.6.0.jar) | The URL or GCS bucket of a Beam job service snapshot. |
| beam-image-enable-pull | false | When set to true, the init action will attempt to pull beam worker images for efficient access later |
| beam-image-version | master | The image version to use when selecting a tagged image |
| beam-image-repository | apache.bintray.io/beam | The image repository root to pull images from. As of September 12th, 2018, these images have not been published yet.  Therefore it is recommended that users build and store their own images when using this init action. |

