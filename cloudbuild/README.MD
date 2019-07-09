# Continuous Integration via Cloud Build

[Cloud Build](https://cloud.google.com/cloud-build/) is configured to trigger on
`/gcbrun` comment from maintainer in PR to the `master` branch.

Cloud Build triggers a set of build steps, configured in a `cloudbuild.yaml`
config file. These steps are:

1.  Create a presubmit Docker container with initialization actions code and
    necessary dependencies installed; this includes git, gcloud, fastunit and
    parametrized.
1.  Push the presubmit Docker container to
    [Container Registry](https://cloud.google.com/container-registry/)
1.  Run the presubmit Docker container on
    [Kubernetes Engine](https://cloud.google.com/kubernetes-engine/) cluster. It
    will execute `presubmit.sh` script that configures test environment,
    determines which tests that need to be run and runs them concurrently.
1.  Delete the presubmit Docker container from Container Registry.

Main responsibilities of `presubmit.sh` script:

1.  Configure `gcloud` to use encrypted SSH key (`ssh-key.enc` and
    `ssh-key.pub.enc` files) to SSH into Dataproc cluster VMs during tests
    execution. This prevents new SSH key generation for each test run.
1.  Only run tests if their corresponding initialization action was modified.
