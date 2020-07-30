#!/bin/bash

set -euxo pipefail

readonly IMAGE=$1
readonly BUILD_ID=$2
readonly DATAPROC_IMAGE_VERSION=$3

readonly POD_NAME=presubmit-${DATAPROC_IMAGE_VERSION//./-}-${BUILD_ID//_/-}

gcloud container clusters get-credentials "${CLOUDSDK_CONTAINER_CLUSTER}"

kubectl run "${POD_NAME}" --generator=run-pod/v1 --image="$IMAGE" \
  --requests "cpu=3,memory=10Gi" --restart=Never \
  --env="COMMIT_SHA=$COMMIT_SHA" \
  --env="IMAGE_VERSION=$DATAPROC_IMAGE_VERSION" \
  --command -- bash /init-actions/cloudbuild/presubmit.sh

trap 'kubectl delete pods "${POD_NAME}"' EXIT

kubectl wait --for=condition=Ready "pod/${POD_NAME}" --timeout=600s

kubectl logs -f "${POD_NAME}"

readonly EXIT_CODE=$(kubectl get pod "${POD_NAME}" \
  -o go-template="{{range .status.containerStatuses}}{{.lastState.terminated.exitCode}}{{end}}")

if [[ ${EXIT_CODE} != 0 ]]; then
  echo "Presubmit failed!"
  kubectl describe "pod/${POD_NAME}"
  exit 1
fi
