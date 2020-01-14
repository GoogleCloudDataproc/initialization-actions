# On-boot commands

This initialization action can be used as either an initialization action or as a startup-script,
with the latter enabling support for commands which need to be re-run on reboot. This
initialization action adds polling for the cluster itself to be fully healthy before issuing
a command, effectively allowing "post-initiailization" actions which can include further setup
or submitting a job.

## Using this initialization action

**:warning: NOTICE:** See [best practices](/README.md#how-initialization-actions-are-used) of using initialization actions in production.

For now, the script relies on polling the Dataproc API to determine an authoritative state
of cluster health on startup, so requires the `--scopes cloud-platform` flag; do not use
this initialization action if you are unwilling to grant your Dataproc clusters' service
account with the `cloud-platform` scope.

The initialization action is designed to log the output of your post-init command into
`/var/log/master-post-init.log`; when debugging, if it doesn't behave as expected, SSH
into the master node and view that logfile for more details. If it doesn't exist, then
you can likely find additional details in the initialization-action or startup-script
logs also in the `/var/log` directory.

## Examples

### Submit a single job with cluster and turn off cluster on completion

Simply modify the `POST_INIT_COMMAND` to whatever actual job submission command you want to run:

    export REGION=<region>
    export CLUSTER_NAME=${USER}-shortlived-cluster
    export POST_INIT_COMMAND=" \
        gcloud dataproc jobs submit hadoop \
            --region ${REGION} \
            --cluster ${CLUSTER_NAME} \
            --jar file:///usr/lib/hadoop-mapreduce/hadoop-mapreduce-examples.jar \
            -- \
            teragen 10000 /tmp/teragen; \
        gcloud dataproc clusters delete -q ${CLUSTER_NAME} --region ${REGION}"
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --scopes cloud-platform \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/post-init/master-post-init.sh \
        --metadata post-init-command="${POST_INIT_COMMAND}"


### Submit a single job with cluster and turn off cluster only if job succeeds

If you replace the semicolon with "&&" before the `gcloud dataproc clusters delete` call, then
the cluster will only be deleted if the job was successful:

    export CLUSTER_NAME=${USER}-shortlived-cluster
    export POST_INIT_COMMAND=" \
        gcloud dataproc jobs submit hadoop \
            --region ${REGION} \
            --cluster ${CLUSTER_NAME} \
            --jar file:///usr/lib/hadoop-mapreduce/hadoop-mapreduce-examples.jar \
            -- \
            teragen 10000 /tmp/teragen && \
        gcloud dataproc clusters delete -q ${CLUSTER_NAME} --region ${REGION}"
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --scopes cloud-platform \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/post-init/master-post-init.sh \
        --metadata post-init-command="${POST_INIT_COMMAND}"

## Create a cluster which automatically resubmits a job on restart or job termination (e.g. for streaming processing)

If you supply this init action as a GCE "startup-script" instead of as an init action, then it
will be run on every (re)boot. This can be leveraged to set up reliable "long-lived" jobs.

Note that individual job incarnations will always be vulnerable to at least some failure mode
by nature (master might suffer unexpected hardware reboot, AppMaster might crash, etc). So,
a "long-lived" job generally won't be a single job incarnation, but rather just ongoing
"resubmission" of a job with the same params on failure. Your application layer is then
responsible for recovering any ongoing state, if applicable.

Dataproc by default will forcibly terminate the YARN applications it detects which appear
to be orphaned from a failed job driver/client program; this makes it safe to resubmit
the job on reboot without manually tracking down orphaned YARN applications which may be
consuming resources for the job that you want to resubmit.

    export REGION=<region>
    export CLUSTER_NAME=${USER}-longrunning-job-cluster
    export POST_INIT_COMMAND=" \
        while true; do \
          gcloud dataproc jobs submit spark \
              --region ${REGION} \
              --cluster ${CLUSTER_NAME} \
              --jar gs://${BUCKET}/my-longlived-job.jar foo args; \
        done"
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --scopes cloud-platform \
        --metadata startup-script-url=gs://goog-dataproc-initialization-actions-${REGION}/post-init/master-post-init.sh,post-init-command="${POST_INIT_COMMAND}"

