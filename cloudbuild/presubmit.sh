#!/bin/bash

set -euxo pipefail

# Declare global variable for passing tests between functions
declare -a TESTS_TO_RUN

configure_gcloud() {
  gcloud config set core/disable_prompts TRUE
  gcloud config set compute/region us-central1
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

# Fetches master branch from GitHub and "resets" local changes to be relative to it,
# so we can diff what changed relatively to master branch.
initialize_git_repo() {
  rm -fr .git
  git config --global init.defaultBranch main
  git init

  git config user.email "ia-tests@presubmit.example.com"
  git config user.name "ia-tests"

  git remote add origin "https://github.com/GoogleCloudDataproc/initialization-actions.git"
  git fetch origin master
  # Fetch all PRs to get history for PRs created from forked repos
  git fetch origin +refs/pull/*/merge:refs/remotes/origin/pr/* > /dev/null 2>&1

  git reset --hard "${COMMIT_SHA}"

  git rebase origin/master
}

# This function adds all changed files to git "index" and diffs them against master branch
# to determine all changed files and looks for tests in directories with changed files.
determine_tests_to_run() {
  # Infer the files that changed
  mapfile -t DELETED_BUILD_FILES < <(git diff origin/master --name-only --diff-filter=D | grep BUILD)
  mapfile -t CHANGED_FILES < <(git diff origin/master --name-only)
  echo "Deleted BUILD files: ${DELETED_BUILD_FILES[*]}"
  echo "Changed files: ${CHANGED_FILES[*]}"

  # Run all tests if common directories modified by deleting files
  if [[ "${#DELETED_BUILD_FILES[@]}" -gt 0 ]]; then
    echo "All tests will be run: the following BUILD files '${DELETED_BUILD_FILES[*]}' were removed"
    TESTS_TO_RUN=(":DataprocInitActionsTestSuite")
    return 0
  fi

  set +x
  # Determines init actions directories that were changed
  declare -a changed_dirs
  for changed_file in "${CHANGED_FILES[@]}"; do
    local changed_dir
    changed_dir="$(dirname "${changed_file}")/"
    # Convert `init/internal/` dir to `init/`
    changed_dir="${changed_dir%%/*}/"
    # Run all tests if common directories modified
    if [[ ${changed_dir} =~ ^(integration_tests|util|cloudbuild)/$ ]]; then
      echo "All tests will be run: '${changed_dir}' was changed"
      TESTS_TO_RUN=(":DataprocInitActionsTestSuite")
      return 0
    fi
    # Hack to workaround empty array expansion on old versions of Bash.
    # See: https://stackoverflow.com/a/7577209/3227693
    if [[ $changed_dir != ./ ]] && [[ ${changed_dirs[*]+" ${changed_dirs[*]} "} != *" ${changed_dir} "* ]]; then
      changed_dirs+=("$changed_dir")
    fi
  done
  echo "Changed directories: ${changed_dirs[*]}"

  # Determines test target in changed init action directories to run
  for changed_dir in "${changed_dirs[@]}"; do
    # NOTE: The ::-1 removes the trailing '/'
    local test_name=${changed_dir::-1}
    # Some of our py_tests (that has dashes in the name) are defined in the top-level directory
    if [[ $test_name == *"-"* ]]; then
      local test_target=":test_${test_name//-/_}"
    else
      local test_target="${test_name}:test_${test_name}"
    fi
    TESTS_TO_RUN+=("${test_target}")
  done
  echo "Tests: ${TESTS_TO_RUN[*]}"

  set -x
}

run_tests() {
  local -r max_parallel_tests=20
  bazel test \
    --jobs="${max_parallel_tests}" \
    --local_test_jobs="${max_parallel_tests}" \
    --flaky_test_attempts=3 \
    --action_env="INTERNAL_IP_SSH=true" \
    --test_output="all" \
    --noshow_progress \
    --noshow_loading_progress \
    --test_arg="--image_version=${IMAGE_VERSION}" \
    "${TESTS_TO_RUN[@]}"
}

main() {
  cd /init-actions
  configure_gcloud
  configure_gcloud_ssh_key
  initialize_git_repo
  determine_tests_to_run
  run_tests
}

main
