#!/bin/bash

set -e

# Ensure key file exists
if [ ! -f "key.json" ]; then
  echo "Error: key.json not found. Please create it."
  echo "Example: gcloud iam service-accounts keys create key.json --iam-account=YOUR-SA@YOUR-PROJECT.iam.gserviceaccount.com --project=YOUR-PROJECT"
  exit 1
fi

# Create the host directory if it doesn't exist and make it writable
HOST_CACHE_DIR="${PWD}/tmp/bazel-cache"
mkdir -p "${HOST_CACHE_DIR}"
chmod 777 "${HOST_CACHE_DIR}"
echo "Host cache directory: ${HOST_CACHE_DIR}"

podman build -f gpu/Dockerfile -t gpu-init-actions-runner:latest .

IMAGE_VERSION="${1:-2.2-debian12}"

time podman run -it --rm \
  --name gpu-test-runner \
  -v ${HOST_CACHE_DIR}:/home/ia-tests/.cache/bazel:Z \
  -e GOOGLE_APPLICATION_CREDENTIALS=/init-actions/key.json \
  -e PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null)}" \
  -e REGION="${REGION:-$(gcloud config get-value compute/region 2>/dev/null)}" \
  --entrypoint /bin/bash \
  gpu-init-actions-runner:latest \
  /init-actions/gpu/run-bazel-tests.sh "$@"