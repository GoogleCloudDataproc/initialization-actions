#!/bin/bash

set -Eeuo pipefail

python3.5 -m pip install -r cloud-build/requirements.pip
pip3 freeze

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
  for file in "${CHANGED_FILES[@]}"
  do
    if [[ $file =~ ^$1 ]]
    then
      return 0
    fi
  done
  return 1
}

# Determines init actions directories that were modified
declare -a DIRECTORIES_TO_TEST
for dir in */
do
  # Skip not init action changes
  if [[ $dir =~ ^(integration_tests/|util/|cloud-build/)$ ]]
  then
    continue
  fi
  if is_prefix "$dir"
  then
    DIRECTORIES_TO_TEST+=("$dir")
  fi
done
echo "Test directories: ${DIRECTORIES_TO_TEST[*]}"

# Determines what tests in modified init action directories to run
declare -a ALL_TESTS
for test_dir in "${DIRECTORIES_TO_TEST[@]}"
do
  if ! tests=$(compgen -G "${test_dir}test*.py")
  then
    echo "ERROR: presubmit failed - can not find tests inside '${test_dir}' directory"
    exit 1
  fi
  mapfile -t tests_array < <(echo "${tests}")
  ALL_TESTS+=("${tests_array[@]}")
done
echo "Tests: ${ALL_TESTS[*]}"

# Run tests of the init actions that were modified
python3.5 -m unittest "${ALL_TESTS[@]}"

exit $?
