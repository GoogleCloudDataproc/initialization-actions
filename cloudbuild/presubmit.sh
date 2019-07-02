#!/bin/bash

set -euxo pipefail

configure_gcloud() {
  gcloud config set core/disable_prompts TRUE
  gcloud config set compute/region us-central1
  gcloud config set compute/zone us-central1-f
}

install_test_dependencies() {
  pip3 install -r integration_tests/requirements.txt
}

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
