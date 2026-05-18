#!/bin/bash

# This script is intended to be used by the Dataproc team to push init action
# modules (e.g., kafka) to the Dataproc init actions GCS bucket. It performs
# necessary validations to make sure the repo is in good shape before pushing.
#
# Usage: ./push-to-gcs.sh <git-ref> <module>
#
# Example: ./push-to-gcs.sh cbde05 kafka

set -euo pipefail

[[ $# -eq 2 ]] || {
  echo "Usage: ./push-to-gcs.sh <git-ref> <module>"
  echo "Example: ./push-to-gcs.sh cbde05 kafka"
  exit 1
}

set -x

readonly HEAD="$1"
readonly MODULE="$2"
readonly GCS_FOLDER=gs://dataproc-initialization-actions/${MODULE}/

function version_le() { [[ "$1" = "$(echo -e "$1\n$2" | sort -V | head -n1)" ]]; }
function version_lt() { [[ "$1" = "$2" ]] && return 1 || version_le "$1" "$2"; }

GCLOUD_SDK_VERSION="$(gcloud --version | awk -F'SDK ' '/Google Cloud SDK/ {print $2}')"
GSUTIL="gcloud storage"
if version_lt "${GCLOUD_SDK_VERSION}" "402.0.0"; then
  GSUTIL="gsutil"
fi

[[ -n "${HEAD}" && -n "${MODULE}" ]]

# Verify the repo has no uncommitted changes.
if ! git diff --exit-code || ! git diff --cached --exit-code; then
  echo "There are uncommitted changes."
  exit 2
fi

# Verify the repo is at the expected HEAD.
if [[ $(git log --format="%H" -n 1) != "${HEAD}"* ]]; then
  echo "The Git repo HEAD is not at $HEAD."
  exit 3
fi

# Verify the module name is valid.
if [[ ! -d "${MODULE}" ]]; then
  echo "Module ${MODULE} was not found."
  exit 4
fi

# Verify shell scripts have permission mode 75x.
for file in "${MODULE}/"*.sh; do
  permissions=$(stat -c '%a' "${file}")
  if [[ ${permissions} != 75* ]]; then
    echo "The permission mode of script ${file} is ${permissions}, expected: 75x."
    exit 5
  fi
done

${GSUTIL} rsync -r --exclude "__pycache__/.*" "${MODULE}/" "${GCS_FOLDER}"

echo "Pushed ${MODULE}/ to ${GCS_FOLDER}."
