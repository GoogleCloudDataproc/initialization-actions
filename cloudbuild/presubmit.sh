#!/bin/bash

set -euxo pipefail

configure_gcloud() {
  gcloud config set core/disable_prompts TRUE
  gcloud config set compute/region us-central1
  gcloud config set compute/zone us-central1-f
}

configure_gcloud_ssh_key() {
  mkdir "${HOME}/.ssh"

  gcloud kms decrypt --location=global --keyring=presubmit --key=presubmit \
    --ciphertext-file=cloudbuild/ssh-key.enc \
    --plaintext-file="${HOME}/.ssh/google_compute_engine"

  gcloud kms decrypt --location=global --keyring=presubmit --key=presubmit \
    --ciphertext-file=cloudbuild/ssh-key.pub.enc \
    --plaintext-file="${HOME}/.ssh/google_compute_engine.pub"

  chmod 600 "${HOME}/.ssh/google_compute_engine"
}

install_test_dependencies() {
  pip3 install -r integration_tests/requirements.txt
}

# Fetches master branch from GitHub and "resets" local changes to be relative to it,
# so we can diff what changed relatively to master branch.
initialize_git_repo() {
  git init

  git remote add origin "https://github.com/GoogleCloudPlatform/dataproc-initialization-actions.git"
  git fetch origin master

  git reset origin/master
}

# This function adds all changed files to git "index" and diffs them against master branch
# to determine all changed files and looks for tests in directories with changed files.
determine_tests_to_run() {
  # Stage files to track their history
  git add --all

  # Infer the files that changed
  mapfile -t CHANGED_FILES < <(git diff --cached origin/master --name-only)
  echo "Changed files: ${CHANGED_FILES[*]}"

  # Determines init actions directories that were changed
  RUN_ALL_TESTS=false
  declare -a changed_dirs
  for changed_file in "${CHANGED_FILES[@]}"; do
    local changed_dir="${changed_file/\/*/}/"
    # Run all tests if common directories were changed
    if [[ ${changed_dir} =~ ^(integration_tests/|util/|cloudbuild/)$ ]]; then
      echo "All tests will be run: '${changed_dir}' was changed"
      RUN_ALL_TESTS=true
      return 0
    fi
    # Hack to workaround empty array expansion on old versions of Bash.
    # See: https://stackoverflow.com/a/7577209/3227693
    if [[ ${changed_dirs[*]+" ${changed_dirs[*]} "} != *" ${changed_dir} "* ]]; then
      changed_dirs+=("$changed_dir")
    fi
  done
  echo "Changed directories: ${changed_dirs[*]}"

  # Determines what tests in changed init action directories to run
  for changed_dir in "${changed_dirs[@]}"; do
    local tests_in_dir
    if ! tests_in_dir=$(compgen -G "${changed_dir}test*.py"); then
      echo "ERROR: presubmit failed - cannot find tests inside '${changed_dir}' directory"
      exit 1
    fi
    declare -a tests_array
    mapfile -t tests_array < <(echo "${tests_in_dir}")
    TESTS_TO_RUN+=("${tests_array[@]}")
  done
  echo "Tests: ${TESTS_TO_RUN[*]}"
}

run_tests() {
  export INTERNAL_IP_SSH=true
  if [[ $RUN_ALL_TESTS == true ]]; then
    # Run all init action tests
    python3 -m fastunit -v
  else
    # Run tests for the init actions that were changed
    python3 -m fastunit -v "${TESTS_TO_RUN[@]}"
  fi
}

main() {
  cd /init-actions
  configure_gcloud
  configure_gcloud_ssh_key
  install_test_dependencies
  initialize_git_repo
  determine_tests_to_run
  run_tests
}

# Declare global variable for passing tests between functions
declare -a TESTS_TO_RUN

main
