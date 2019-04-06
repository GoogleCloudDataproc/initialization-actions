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
 
## Parallel tests
By default, tests are executed synchronously - this is standard `unittest` module behaviour and works well for
real unit tests. 

However here tests are taking significant amount of time and running them one by one 
sometimes takes up to 2 hours to test whole component. 

This can be speed up by using [python3-fastunit](https://github.com/ityoung/python3-fastunit) 
package. Once installed (there is no PIP version) tests can be run as simple as before, but switching
`unittest` module to `fastunit`:

 `python -m fastunit hive-hcatalog.test_hive`