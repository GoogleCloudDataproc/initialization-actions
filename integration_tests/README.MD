# Integration tests framework

This directory contains integration tests framework that runs init-actions on
real Dataproc cluster and validates if software is properly installed.

## Requirements

In order to run tests you need [Bazel](https://bazel.build/).

You also need to have `gcloud` CLI configured to use your GCP project.

## Running tests

To run all tests for default image version use command in project root:
```bash
bazel test :DataprocInitActionsTestSuite --jobs=25
```

To run all tests for default image version use command in project root:
```bash
bazel test :DataprocInitActionsTestSuite --test_arg=--image_version=1.4
```

To run tests for specific initialization action use its package name and test name:
```bash
bazel test gpu:test_gpu
```
