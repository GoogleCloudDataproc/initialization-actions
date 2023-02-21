#!/bin/bash

set -euxo pipefail

readonly IMAGE=$1
readonly BUILD_ID=$2
readonly DATAPROC_IMAGE_VERSION=$3

readonly POD_NAME=presubmit-${DATAPROC_IMAGE_VERSION//./-}-${BUILD_ID//_/-}

gcloud container clusters get-credentials "${CLOUDSDK_CONTAINER_CLUSTER}"

LOGS_SINCE_TIME=$(date --iso-8601=seconds)

#kubectl run "${POD_NAME}" \
#  --image="${IMAGE}" \
#  --restart=Never \
#  --env="COMMIT_SHA=${COMMIT_SHA}" \
#  --env="IMAGE_VERSION=${DATAPROC_IMAGE_VERSION}" \
#  --command -- bash /init-actions/cloudbuild/presubmit.sh
export IMAGE_NAME=${POD_NAME}
export COMMIT_SHA=${COMMIT_SHA}
export IMAGE_VERSION=${DATAPROC_IMAGE_VERSION}
export IMAGE_BUILD_ID=${BUILD_ID}
export IMAGE_BUILD_ID=gcr.io/cloud-dataproc-ci/init-actions-image:${BUILD_ID}

cat cloudbuild/deployment.yaml

cat cloudbuild/deployment.yaml |sed "s/{{IMAGE_NAME}}/${IMAGE_NAME}/g;s/{{COMMIT_SHA}}/${COMMIT_SHA}/g;s/{{IMAGE_VERSION}}/${IMAGE_VERSION}/g;s/{{IMAGE_BUILD_ID}}|${IMAGE_BUILD_ID}/g" | kubectl apply -f -

# Delete POD on exit and describe it before deletion if exit was unsuccessful
trap '[[ $? != 0 ]] && kubectl describe "pod/${POD_NAME}"; kubectl delete pods "${POD_NAME}"' EXIT

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
