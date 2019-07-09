# Continuous Integration via Cloud Build

[Cloud Build](https://cloud.google.com/cloud-build/) is configured to trigger on
`/gcbrun` comment from maintainer in PR to the `master` branch.

Cloud Build triggers a set of build steps, configured in a `cloudbuild.yaml`
config file. These steps are:

1.  Create a presubmit Docker container with initialization actions code and
    necessary dependencies installed; this includes git, gcloud, fastunit and
    parametrized.
1.  Push a presubmit Docker container to
    [Container Registry](https://cloud.google.com/container-registry/)
1.  Run a presubmit Docker container on
    [Kubernetes Engine](https://cloud.google.com/kubernetes-engine/) cluster. It
    will execute `presubmit.sh` script that determines the tests that need to be
    run and runs them concurrently.
1.  Delete a presubmit Docker container from Container Registry.

Tests only run if their corresponding initialization action was modified. This
logic is contained in `presubmit.sh` script.
