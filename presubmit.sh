#!/bin/bash

set -Eeuo pipefail

git init

# Stage files to track their history
git add .

git remote add origin "https://github.com/GoogleCloudPlatform/dataproc-initialization-actions.git"
git fetch origin master

# Infer the files that changed
mapfile -t CHANGED_FILES < <(git diff origin/master --name-only)
echo "Changed files: ${CHANGED_FILES[*]}"

# Determines whether a given string is a substring of any changed file name
contains() {
  for file in "${CHANGED_FILES[@]}"
  do
    if [[ $file =~ $1 ]]; then
      return 0
    fi
  done
  return 1
}

declare -a DIRECTORIES_TO_TEST
for dir in */
do
  # skip not init action changes
  if [[ $dir =~ ^(integration_tests/|util/)$ ]]; then
    continue
  fi
  if contains "$dir"; then
    DIRECTORIES_TO_TEST+=("$dir")
  fi
done
echo "Test directories: ${DIRECTORIES_TO_TEST[*]}"

#mapfile -t -d $'\0' TESTS < <(printf "%stest.py\0" "${DIRECTORIES_TO_TEST[@]}")

declare -a TESTS
for test_dir in "${DIRECTORIES_TO_TEST[@]}"
do
  for test in "${test_dir}"test*.py
  do
    TESTS+=("$test")
  done
done
echo "Tests: ${TESTS[*]}"

isSuccess=0

# Run only the tests of the init actions that were modified
python "${TESTS[@]}" || isSuccess=1

exit $isSuccess
