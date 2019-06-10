# Continuous Integration via Cloud Build

Cloud Build is configured in our GCP project (google.com:cloud-dataproc-ci) to trigger whenever there are PRs from a fork to the main repository. We are using GitHub App Triggers Alpha to integrate with GitHub.

Cloud Build triggers a set of build steps, configured in a yaml config file. These steps are:

1. Create a Docker container with our initialization actions code and necessary dependencies; this includes git, gcloud and parametrized.
1. Launch a presubmit shell script that is responsible for determining the tests to be run and consequently running them.

Tests only run if their corresponding initialization action was modified. This logic is contained within presubmit.sh.

For more details follow this [link](https://docs.google.com/document/d/1fS5dBGGMsfdyyNg4FkjQ2ZaluF5GnLTd-z8O2lcuemI/edit#heading=h.amhicoppzmhm).
