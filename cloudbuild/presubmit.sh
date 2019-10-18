#!/bin/bash

set -euxo pipefail

configure_gcloud() {
  gcloud config set core/disable_prompts TRUE
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
  git config user.email "ia-tests@presubmit.example.com"
  git config user.name "ia-tests"

  git remote add origin "https://github.com/GoogleCloudPlatform/dataproc-initialization-actions.git"
  git fetch origin master
  # Fetch all PRs to get history for PRs created from forked repos
  git fetch origin +refs/pull/*/merge:refs/remotes/origin/pr/*

  git reset "${COMMIT_SHA}"

  # git rebase origin/master
}

# Determines if an element is in a list
contains() {
    [[ $1 =~ (^|[[:space:]])$2($|[[:space:]]) ]] && exit 0 || exit 1
}

# This function adds all changed files to git "index" and diffs them against master branch
# to determine all changed files and looks for tests in directories with changed files.
determine_tests_to_run() {
  # Infer the files that changed
  mapfile -t CHANGED_FILES < <(git diff origin/master --name-only)
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

  SPECIAL_INIT_ACTIONS=(cloud-sql-proxy/ starburst-presto/ hive-hcatalog/)

  # Determines what tests in changed init action directories to run
  for changed_dir in "${changed_dirs[@]}"; do
    local tests_in_dir
    if ! tests_in_dir=$(compgen -G "${changed_dir}test*.py"); then
      echo "ERROR: presubmit failed - cannot find tests inside '${changed_dir}' directory"
      exit 1
    fi
    declare -a tests_array
    if contains $changed_dir SPECIAL_INIT_ACTIONS; then
      # Some of our py_tests are defined in the top-level directory
      mapfile -t tests_array < <(echo ":test_${changed_dir::-1}")
    else 
      mapfile -t tests_array < <(echo "${changed_dir::-1}:test_${changed_dir::-1}")
    fi
    TESTS_TO_RUN+=("${tests_array[@]}")
  done
  echo "Tests: ${TESTS_TO_RUN[*]}"
}

run_tests() {
  export INTERNAL_IP_SSH=true
  if [[ $RUN_ALL_TESTS == true ]]; then
    # Run all init action tests
    bazel test :DataprocInitActionsTestSuite --jobs 15
  else
    # Run tests for the init actions that were changed
    bazel test "${TESTS_TO_RUN[@]}"
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
