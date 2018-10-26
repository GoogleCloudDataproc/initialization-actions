# Integration tests framework
This directory contains integration tests framework that runs init-actions on real Dataproc cluster 
and validates if software is properly installed.


## Requirements
In order to run tests you need `Python 3.5` virtualenv and libs specified in `requirements.txt`.

You also need to have `gcloud` CLI configured to use your GCP project.

## Running tests
To run all tests just type `python -m unittest` from project root.

To run test for specific init action use its package name and test name:
 `python -m unittest hive-hcatalog.test_hive`
 