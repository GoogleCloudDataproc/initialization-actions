# Continuous Integration via Cloud Build

[Cloud Build](https://cloud.google.com/cloud-build/) is configured to trigger
whenever there are PRs from a fork to the main repository.

Cloud Build triggers a set of build steps, configured in a `cloudbuild.yaml`
config file. These steps are:

1.  Create a Docker container with our initialization actions code and necessary
    dependencies; this includes git, gcloud, fastunit and parametrized.
1.  Launch a presubmit shell script that is responsible for determining the
    tests to be run and concurrently running them.

Tests only run if their corresponding initialization action was modified. This
logic is contained within `presubmit.sh`.
