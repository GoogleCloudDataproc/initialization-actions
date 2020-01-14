# Beam Initialization Actions

While Apache Beam is primarily an SDK, jobs running under its portability
framework require a job service to run correctly.  This directory contains a
setup script to properly configure beam job services.

**WARNING:** The Beam portability framework is **under active development** and
should not be used in a production context.  It is also not supported on
clusters running in [high availability](https://cloud.google.com/dataproc/docs/concepts/configuring-clusters/high-availability) 
mode.

Due to the current development
[status](https://beam.apache.org/contribute/portability/#status) of Beam's
portability framework, you are responsible for building and maintaining their
own Beam artifacts manually. Instructions are included below.

## Using this initialization action

**:warning: NOTICE:** See [best practices](/README.md#how-initialization-actions-are-used) of using initialization actions in production.

## Building Beam Artifacts

You will generate two categories of artifacts for this initialization action:

| Job Service | This micro service runs on the master node, accepting new beam jobs on port 8099.  It is configured to submit Beam jobs to Apache Flink. |
| Worker Container Images | These docker images run language-specific code on worker nodes. |

When building manually, substitute the following terms into build commands. In
bash, set environment variables using `export <term>=<value>`.

| `BEAM_JOB_SERVICE_DESTINATION` | A Cloud Storage directory path accessible by both the build machine and the cluster to be created. |
| `BEAM_CONTAINER_IMAGE_DESTINATION` | A Docker repository path prefix accessible by both the build machine and the cluster to be created. |
| `BEAM_SOURCE_VERSION` | A tag, branch, or commit hash in the Beam source repositories to build artifacts from. (default: `master`) |

### Automated Build

You can invoke a helper script from `util` directory to build Beam artifacts.

```bash
bash ./util/build-beam-artifacts.sh <BEAM_JOB_SERVICE_DESTINATION> <BEAM_CONTAINER_IMAGE_DESTINATION> [<BEAM_SOURCE_VERSION>]
```

### Manual Build

To get started, clone the beam source code into a working directory.

```bash
git clone https://github.com/apache/beam.git
cd beam
git checkout ${BEAM_SOURCE_VERSION}
```

#### Build the Job Service

Next, build a standalone job service jar.

```bash
./gradlew :beam-runners-flink_2.11-job-server:shadowJar
```

Then, upload the jar to a Cloud Storage path that clusters can access during
initialization.

```bash
gsutil cp \
  ./runners/flink/job-server/build/libs/beam-runners-flink_2.11-job-server-*-SNAPSHOT.jar \
  <BEAM_JOB_SERVICE_DESTINATION>/beam-runners-flink_2.11-job-server-latest-SNAPSHOT.jar
```

#### Build the Worker Container Images

Build the docker images used by Beam worker tasks.

```bash
./gradlew docker
```

Docker images names are generated in the following format:
`<USER>-docker-apache.bintray.io/beam/<LANGUAGE>`. 

Rename and push the images to a docker repository path that clusters can access
during initialization.  As a best practice, tag the images with the
`BEAM_SOURCE_VERSION` they were generated from.

```bash
docker tag \
  <USER>-docker-apache.bintray.io/beam/<LANGUAGE> \
  <BEAM_CONTAINER_IMAGE_DESTINATION>/<LANGUAGE>:<BEAM_SOURCE_VERSION>
docker push <BEAM_CONTAINER_IMAGE_DESTINATION>/<LANGUAGE>:<BEAM_SOURCE_VERSION>
```

## Create a Beam Cluster

You create a Beam cluster by calling the Cloud Dataproc clusters create command with the following initialization actions:

  - `docker/docker.sh`
  - `flink/flink.sh`
  - `beam/beam.sh`

The Beam `beam` and `flink/flink.sh` initialization actions use the following
[metadata variables](https://cloud.google.com/dataproc/docs/concepts/configuring-clusters/init-actions#passing_arguments_to_initialization_actions):

| Metadata Key | Default | Description |
| ------------ | ------- | ----------- |
| beam-job-service-snapshot | [v2.6.0](http://repo1.maven.org/maven2/org/apache/beam/beam-runners-flink_2.11-job-server/2.6.0/beam-runners-flink_2.11-job-server-2.6.0.jar) | The Cloud Storage path of your JobService jar (see above) |
| beam-artifacts-gcs-path | `<cluster staging bucket>` | A cluster-writeable GCS path to store beam artifacts under |
| beam-image-enable-pull | false | When set to true, the init action will attempt to pull beam worker images for efficient access later |
| beam-image-version | master | The image version to use when selecting a tagged image |
| beam-image-repository | apache.bintray.io/beam | The image repository root to pull images from. As of September 12th, 2018, these images have not been published yet.  Therefore it is recommended that you build and store their own images when using this init action. |
| flink-start-yarn-session | `true` | Run a flink session in YARN on startup. |
| flink-snapshot-url | `<none>` | URL to a Flink snapshot. |

You should explicitly set the Beam and Flink metadata variables (use a script as
shown later).

```bash
REGION=<region>
CLUSTER_NAME="$1"
INIT_ACTIONS="gs://goog-dataproc-initialization-actions-${REGION}/docker/docker.sh"
INIT_ACTIONS+=",gs://goog-dataproc-initialization-actions-${REGION}/flink/flink.sh"
INIT_ACTIONS+=",gs://goog-dataproc-initialization-actions-${REGION}/beam/beam.sh"
FLINK_SNAPSHOT="https://archive.apache.org/dist/flink/flink-1.5.3/flink-1.5.3-bin-hadoop28-scala_2.11.tgz"
METADATA="beam-job-service-snapshot=<...>"
METADATA+=",beam-image-enable-pull=true"
METADATA+=",beam-image-repository=<...>"
METADATA+=",beam-image-version=latest"
METADATA+=",flink-start-yarn-session=true"
METADATA+=",flink-snapshot-url=${FLINK_SNAPSHOT}"

gcloud dataproc clusters create "${CLUSTER_NAME}" \
    --initialization-actions "${INIT_ACTIONS}" \
    --image-version "1.2" \
    --metadata "${METADATA}"
```

The Beam Job Service runs on port `8099` of the master node. You can submit
portable Beam jobs against this port. For example, to run the [Go Wordcount
example](https://github.com/apache/beam/tree/master/sdks/go/examples/wordcount)
on the master node, upload the wordcount job binary, and then run:

```bash
./wordcount \
    --runner flink \
    --endpoint localhost:8099 \
    --experiments beam_fn_api \
    --output=<out> \
    --container_image <BEAM_CONTAINER_DESTINATION>/go:<BEAM_SOURCE_VERSION>
```

The Beam Job Service port must be opened to submit beam jobs from machines
outside the cluster (see [Using Firewall
Rules](https://cloud.google.com/vpc/docs/using-firewalls) for instructions)).
