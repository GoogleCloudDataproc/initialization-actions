#!/bin/bash

set -euxo pipefail

readonly IMAGE=$1
readonly BUILD_ID=$2
readonly DATAPROC_IMAGE_VERSION=$3

readonly POD_NAME=presubmit-${DATAPROC_IMAGE_VERSION//./-}-${BUILD_ID//_/-}

gcloud container clusters get-credentials "${CLOUDSDK_CONTAINER_CLUSTER}"

ARGS=("--generator=run-pod/v1"
	  "--image=$IMAGE"
	  "--requests"
	  "cpu=4,memory=12Gi"
	  "--restart=Never"
	  "--env=IMAGE_VERSION=$DATAPROC_IMAGE_VERSION"
	  "--env=BUILD_ID=$BUILD_ID")

if [[ "${RUN_ALL_TESTS}" == "true" ]]; then
	ARGS+=("--env=RUN_ALL_TESTS=true")
fi

kubectl run "${POD_NAME}" "${ARGS[@]}" \
	--command -- bash /init-actions/cloudbuild/run-tests.sh

trap 'kubectl delete pods "${POD_NAME}"' EXIT

kubectl wait --for=condition=Ready "pod/${POD_NAME}" --timeout=600s

kubectl logs -f "${POD_NAME}"

readonly EXIT_CODE=$(kubectl get pod "${POD_NAME}" \
  -o go-template="{{range .status.containerStatuses}}{{.state.terminated.exitCode}}{{end}}")

if [[ ${EXIT_CODE} != 0 ]]; then
  echo "Presubmit failed!"
  exit 1
fi
