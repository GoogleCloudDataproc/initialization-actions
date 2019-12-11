#!/bin/bash
#
# Can be used either under --metadata startup-script-url=... or with
# --initialization-actions to invoke commands (like submitting a job)
# after cluster becomes initially healthy and then on every master
# reboot or a single time on cluster deployment, respectively.
#
# Examples:
#   Submit a single job with cluster and turn off cluster on completion:
#
#     gcloud dataproc clusters create ${CLUSTER_NAME} --scopes cloud-platform --initialization-actions gs://${BUCKET}/master-post-init.sh --metadata post-init-command="gcloud dataproc jobs submit hadoop --cluster ${CLUSTER_NAME} --jar file:///usr/lib/hadoop-mapreduce/hadoop-mapreduce-examples.jar teragen 10000 /tmp/teragen; gcloud dataproc clusters delete -q ${CLUSTER_NAME}"
#
#   Create a cluster which runs a "forever-running" job, resubmitting on reboot
#   or if the job terminates for any reason:
#
#     gcloud dataproc clusters create ${CLUSTER_NAME} --scopes cloud-platform --metadata startup-script-url=gs://${BUCKET}/master-post-init.sh,post-init-command="while true; do gcloud dataproc jobs submit spark --cluster ${CLUSTER_NAME} --jar gs://${BUCKET}/my-longlived-job.jar foo args; done"

METADATA_ROOT='http://metadata/computeMetadata/v1/instance/attributes'

# Only run on master. If you want to run on all nodes instead, then simply
# remove these lines which exit for non-master nodes.
ROLE=$(curl -f -s -H Metadata-Flavor:Google ${METADATA_ROOT}/dataproc-role)
if [[ "${ROLE}" != 'Master' ]]; then
  exit 0
fi

CLUSTER_NAME=$(curl -f -s -H Metadata-Flavor:Google \
  ${METADATA_ROOT}/dataproc-cluster-name)

# Fetch the actual command we want to run once the cluster is healthy.
# The command is specified with the 'post-init-command' key.
POST_INIT_COMMAND=$(curl -f -s -H Metadata-Flavor:Google \
  ${METADATA_ROOT}/post-init-command)

if [ -z ${POST_INIT_COMMAND} ]; then
  echo "Failed to find metadata key 'post-init-command'"
  exit 1
fi

# We must put the bulk of the login in a separate helper script so that we can
# 'nohup' it.
cat <<EOF >/usr/local/bin/await_cluster_and_run_command.sh
#!/bin/bash

# Helper to get current cluster state.
function get_cluster_state() {
  echo \$(gcloud dataproc clusters describe ${CLUSTER_NAME} | \
      grep -A 1 "^status:" | grep "state:" | cut -d ':' -f 2)
}

# Helper which waits for RUNNING state before issuing the command.
function await_and_submit() {
  local cluster_state=\$(get_cluster_state)
  echo "Current cluster state: \${cluster_state}"
  while [[ "\${cluster_state}" != 'RUNNING' ]]; do
    echo "Sleeping to await cluster health..."
    sleep 5
    local cluster_state=\$(get_cluster_state)
    if [[ "\${cluster_state}" == 'ERROR' ]]; then
      echo "Giving up due to cluster state '\${cluster_state}'"
      exit 1
    fi
  done

  ${POST_INIT_COMMAND}
}

await_and_submit
EOF

chmod 750 /usr/local/bin/await_cluster_and_run_command.sh

# Uncomment this following line and comment out the line after it to throw away
# the stdout/stderr of the command instead of logging it.
#nohup /usr/local/bin/await_cluster_and_run_command.sh &>> /dev/null &
nohup /usr/local/bin/await_cluster_and_run_command.sh &>>/var/log/master-post-init.log \
  ;
