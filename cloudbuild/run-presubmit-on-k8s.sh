#!/bin/bash

set -euxo pipefail

readonly IMAGE=$1
readonly BUILD_ID=$2
readonly DATAPROC_IMAGE_VERSION=$3

readonly POD_NAME=presubmit-${DATAPROC_IMAGE_VERSION//./-}-${BUILD_ID//_/-}

gcloud container clusters get-credentials "${CLOUDSDK_CONTAINER_CLUSTER}"

kubectl run "${POD_NAME}" --generator=run-pod/v1 --image="$IMAGE" \
  --requests "cpu=2,memory=4Gi" --restart=Never \
  --env="COMMIT_SHA=$COMMIT_SHA" \
  --env="IMAGE_VERSION=$DATAPROC_IMAGE_VERSION" \
  --command -- bash /init-actions/cloudbuild/presubmit.sh

# Delete POD on exit and desribe it before deletion if exit was unsuccessful
trap '[[ $? != 0 ]] && kubectl describe "pod/${POD_NAME}"; kubectl delete pods "${POD_NAME}"' EXIT

kubectl wait --for=condition=Ready "pod/${POD_NAME}" --timeout=600s

kubectl logs -f "${POD_NAME}"

# Wait until POD will be terminated
wait_secs=60
while ((wait_secs > 0)) && ! kubectl describe "pod/${POD_NAME}" | grep -q Terminated; do
  sleep 5
  ((wait_secs-=5))
done

readonly EXIT_CODE=$(kubectl get pod "${POD_NAME}" \
  -o go-template="{{range .status.containerStatuses}}{{.state.terminated.exitCode}}{{end}}")

if [[ ${EXIT_CODE} != 0 ]]; then
  echo "Presubmit failed!"
  exit 1
fi
