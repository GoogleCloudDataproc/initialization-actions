# Integration tests framework

This directory contains integration tests framework that runs init-actions on
real Dataproc cluster and validates if software is properly installed.

## Requirements

In order to run tests you need Python with
[setuptools](https://pypi.org/project/setuptools/) installed and
[Bazel](https://bazel.build/).

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

To run tests for specific initialization action use its package name and test
name:

```bash
bazel test gpu:test_gpu
```

## Test flags

-   `--image=<URI>` - an optional flag that specifies an image URI that will be
    used for testing. When using this flag you need to set `--image-version`
    flag too, so tests will know what image version is under test.
-   `--image_version=<version>` - a required flag that specifies an image
    version that will be used for testing.
-   `--skip_cleanup=<true|false>` - an optional flag that specifies whether to
    clean up a test cluster after the test run, could be useful for debugging.
