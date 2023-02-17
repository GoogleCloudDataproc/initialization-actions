#!/bin/bash

set -euxo pipefail

readonly IMAGE=$1
readonly BUILD_ID=$2
readonly DATAPROC_IMAGE_VERSION=$3

readonly POD_NAME=presubmit-${DATAPROC_IMAGE_VERSION//./-}-${BUILD_ID//_/-}

gcloud container clusters get-credentials "${CLOUDSDK_CONTAINER_CLUSTER}"

LOGS_SINCE_TIME=$(date --iso-8601=seconds)

kubectl run "${POD_NAME}" \
  --image="${IMAGE}" \
  --restart=Never \
  --env="COMMIT_SHA=${COMMIT_SHA}" \
  --env="IMAGE_VERSION=${DATAPROC_IMAGE_VERSION}" \
  --command -- bash /init-actions/cloudbuild/presubmit.sh

# Delete POD on exit and describe it before deletion if exit was unsuccessful
trap '[[ $? != 0 ]] && kubectl describe "pod/${POD_NAME}"; kubectl delete pods "${POD_NAME}"' EXIT

kubectl set resources deployment ${POD_NAME}   --limits=cpu=750m,memory=2Gi,ephemeral-storage=2Gi --requests=cpu=750m,memory=2Gi,ephemeral-storage=2Gi

kubectl wait --for=condition=Ready "pod/${POD_NAME}" --timeout=15m

while ! kubectl describe "pod/${POD_NAME}" | grep -q Terminated; do
  kubectl logs -f "${POD_NAME}" --since-time="${LOGS_SINCE_TIME}" --timestamps=true
  LOGS_SINCE_TIME=$(date --iso-8601=seconds)
done


EXIT_CODE=$(kubectl get pod "${POD_NAME}" \
  -o go-template="{{range .status.containerStatuses}}{{.state.terminated.exitCode}}{{end}}")

if [[ ${EXIT_CODE} != 0 ]]; then
  echo "Presubmit failed!"
  exit 1
fi
