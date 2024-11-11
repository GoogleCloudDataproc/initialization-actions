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
