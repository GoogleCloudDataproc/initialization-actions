#!/bin/bash

set -euo pipefail

git init

# Stage files to track their history
git add .

git remote add origin "https://github.com/GoogleCloudPlatform/dataproc-initialization-actions.git"
git fetch origin master

# Infer the files that changed
mapfile -t CHANGED_FILES < <(git diff origin/master --name-only)
echo "Changed files: ${CHANGED_FILES[*]}"

# Determines whether a given string is a prefix string of any changed file name
is_prefix() {
  for file in "${CHANGED_FILES[@]}"; do
    if [[ $file =~ ^$1 ]]; then
      return 0
    fi
  done
  return 1
}

# Determines init actions directories that were modified
RUN_ALL_TESTS=false
declare -a DIRECTORIES_TO_TEST
for dir in */; do
  # Run all tests if common directories were modified
  if [[ $dir =~ ^(integration_tests/|util/|cloudbuild/)$ ]]; then
    echo "All tests will be run: '$dir' was modified"
    RUN_ALL_TESTS=true
    break
  fi
  if is_prefix "$dir"; then
    DIRECTORIES_TO_TEST+=("$dir")
  fi
done
echo "Test directories: ${DIRECTORIES_TO_TEST[*]}"

# Determines what tests in modified init action directories to run
declare -a TESTS_TO_RUN
for test_dir in "${DIRECTORIES_TO_TEST[@]}"; do
  if ! tests=$(compgen -G "${test_dir}test*.py"); then
    echo "ERROR: presubmit failed - cannot find tests inside '${test_dir}' directory"
    exit 1
  fi
  mapfile -t tests_array < <(echo "${tests}")
  TESTS_TO_RUN+=("${tests_array[@]}")
done
echo "Tests: ${TESTS_TO_RUN[*]}"

# Run tests of the init actions that were modified
if [[ $RUN_ALL_TESTS == true ]]; then
  python3 -m fastunit
elif [[ ${#TESTS_TO_RUN[@]} != 0 ]]; then
  python3 -m fastunit "${TESTS_TO_RUN[@]}"
fi
