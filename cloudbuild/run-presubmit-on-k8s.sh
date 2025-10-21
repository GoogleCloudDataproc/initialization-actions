#!/bin/bash

set -euxo pipefail

readonly IMAGE=$1
readonly BUILD_ID=$2
readonly DATAPROC_IMAGE_VERSION=$3

readonly POD_NAME=presubmit-${DATAPROC_IMAGE_VERSION//./-}-${BUILD_ID//_/-}

gcloud container clusters get-credentials "${CLOUDSDK_CONTAINER_CLUSTER}"

LOGS_SINCE_TIME=$(date --iso-8601=seconds)

# This kubectl sometimes fails because services have not caught up.  Thread.yield()
sleep 10s

readonly POD_CONFIG="${POD_NAME}-config.yaml"
cat >$POD_CONFIG <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: "${POD_NAME}"
spec:
  restartPolicy: Never
  containers:
  - name: "dataproc-test-runner"
    image: "${IMAGE}"
    resources:
      requests:
        memory: "4G"
        cpu: "6000m"
    env:
    - name: COMMIT_SHA
      value: "${COMMIT_SHA}"
    - name: IMAGE_VERSION
      value: "${DATAPROC_IMAGE_VERSION}"
    command: ["bash"]
    args: ["/init-actions/cloudbuild/presubmit.sh"]
EOF

kubectl apply -f $POD_CONFIG

# Delete POD on exit and describe it before deletion if exit was unsuccessful
trap 'exit_code=$?
if [[ ${exit_code} != 0 ]]; then
  echo "Presubmit failed for ${POD_NAME}. Describing pod..."
  kubectl describe "pod/${POD_NAME}" || echo "Failed to describe pod."

  PROJECT_ID=$(gcloud config get-value project 2>/dev/null || echo "unknown-project")
  BUCKET="dataproc-init-actions-test-${PROJECT_ID}"
  LOG_GCS_PATH="gs://${BUCKET}/${BUILD_ID}/logs/${POD_NAME}.log"

  echo "Attempting to upload logs to ${LOG_GCS_PATH}"
  if kubectl logs "${POD_NAME}" | gsutil cp - "${LOG_GCS_PATH}"; then
    echo "Logs for failed pod ${POD_NAME} uploaded to: ${LOG_GCS_PATH}"
  else
    echo "Log upload to ${LOG_GCS_PATH} failed."
  fi
fi
echo "Deleting pod ${POD_NAME}..."
kubectl delete pods "${POD_NAME}" --ignore-not-found=true
exit ${exit_code}' EXIT

kubectl wait --for=condition=Ready "pod/${POD_NAME}" --timeout=15m

# To mitigate problems with early test failure, retry kubectl logs
sleep 10s
while true; do
  if ! kubectl describe "pod/${POD_NAME}" > /dev/null 2>&1; then
    echo "Pod ${POD_NAME} not found, assuming it has been deleted."
    break # Exit the loop if the pod doesn't exist
  fi

  if kubectl describe "pod/${POD_NAME}" | grep -q Terminated; then
    echo "Pod ${POD_NAME} is Terminated."
    break # Exit the loop if the pod is Terminated
  fi

  # Try to stream logs
  kubectl logs -f "${POD_NAME}" --since-time="${LOGS_SINCE_TIME}" --timestamps=true || true
  LOGS_SINCE_TIME=$(date --iso-8601=seconds)
  sleep 2
done

# Final check on the pod exit code
EXIT_CODE=$(kubectl get pod "${POD_NAME}" -o go-template="{{range .status.containerStatuses}}{{.state.terminated.exitCode}}{{end}}" || echo "1")

if [[ ${EXIT_CODE} != 0 ]]; then
  echo "Presubmit final state for ${POD_NAME} indicates failure (Exit Code: ${EXIT_CODE})."
  # The trap will handle the log upload and cleanup
  exit 1
fi

echo "Presubmit for ${POD_NAME} successful."
# Explicitly exit 0 to clear the trap's exit code
exit 0
