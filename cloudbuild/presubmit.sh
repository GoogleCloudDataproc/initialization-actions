#!/bin/bash

set -euxo pipefail

apt-get update
apt-get install -y telnet nmap net-tools

#nmap -Pn 35.243.182.226
#telnet 35.243.182.226 22

ifconfig

mkdir /builder/home/.ssh

gcloud kms decrypt --location=global --keyring=presubmit --key=presubmit \
    --ciphertext-file=cloudbuild/ssh-key.enc \
    --plaintext-file=/builder/home/.ssh/google_compute_engine

gcloud kms decrypt --location=global --keyring=presubmit --key=presubmit \
    --ciphertext-file=cloudbuild/ssh-key.pub.enc \
    --plaintext-file=/builder/home/.ssh/google_compute_engine.pub

chmod 600 /builder/home/.ssh/google_compute_engine

gcloud compute firewall-rules list

gcloud beta compute ssh idv-test-m --project=cloud-dataproc-ci --zone=us-east1-b --internal-ip \
    --verbosity=debug --command="uname -a; pwd" -- -T -vvv

configure_gcloud() {
  gcloud config set core/disable_prompts TRUE
  gcloud config set compute/region us-central1
  gcloud config set compute/zone us-central1-f
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

# Determines whether a given string is a prefix string of any changed file name
is_prefix() {
  for file in "${CHANGED_FILES[@]}"; do
    if [[ $file =~ ^$1 ]]; then
      return 0
    fi
  done
  return 1
}

# This function adds all changed files to git "index" and diffs them against master branch
# to determine all modified files and looks for tests in directories with modified files.
determine_tests_to_run() {
  # Stage files to track their history
  git add --all

  # Infer the files that changed
  mapfile -t CHANGED_FILES < <(git diff --cached origin/master --name-only)
  echo "Changed files: ${CHANGED_FILES[*]}"

  # Determines init actions directories that were modified
  RUN_ALL_TESTS=false
  local -a modified_dirs
  for dir in */; do
    # Run all tests if common directories were modified
    if [[ $dir =~ ^(integration_tests/|util/|cloudbuild/)$ ]]; then
      echo "All tests will be run: '$dir' was modified"
      RUN_ALL_TESTS=true
      return 0
    fi
    if is_prefix "$dir"; then
      modified_dirs+=("$dir")
    fi
  done
  echo "Modified directories: ${modified_dirs[*]}"

  # Determines what tests in modified init action directories to run
  declare -a TESTS_TO_RUN
  for modified_dir in "${modified_dirs[@]}"; do
    local tests_in_dir
    if ! tests_in_dir=$(compgen -G "${modified_dir}test*.py"); then
      echo "ERROR: presubmit failed - cannot find tests inside '${modified_dir}' directory"
      exit 1
    fi
    local -a tests_array
    mapfile -t tests_array < <(echo "${tests_in_dir}")
    TESTS_TO_RUN+=("${tests_array[@]}")
  done
  echo "Tests: ${TESTS_TO_RUN[*]}"
}

run_tests() {
  if [[ $RUN_ALL_TESTS == true ]]; then
    # Run all init action tests
    python3 -m fastunit -v
  else
    # Run tests of the init actions that were modified
    python3 -m fastunit -v "${TESTS_TO_RUN[@]}"
  fi
}

main() {
  configure_gcloud
  install_test_dependencies
  initialize_git_repo
  determine_tests_to_run
  run_tests
}

main
