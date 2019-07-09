#!/bin/bash

set -euxo pipefail

readonly IMAGE=$1
readonly BUILD_ID=$2

readonly POD_NAME=presubmit-${BUILD_ID}

gcloud container clusters get-credentials "${CLOUDSDK_CONTAINER_CLUSTER}"

kubectl run "${POD_NAME}" --generator=run-pod/v1 --image="$IMAGE" \
  --requests "cpu=2,memory=2Gi" --restart=Never \
  --command -- bash /init-actions/cloudbuild/presubmit.sh

trap 'kubectl delete pods "${POD_NAME}"' EXIT

kubectl wait --for=condition=Ready "pod/${POD_NAME}" --timeout=600s

kubectl logs -f "${POD_NAME}"

readonly EXIT_CODE=$(kubectl get pod "${POD_NAME}" \
  -o go-template="{{range .status.containerStatuses}}{{.state.terminated.exitCode}}{{end}}")

if [[ ${EXIT_CODE} != 0 ]]; then
  echo "Presubmit failed!"
  exit 1
fi
