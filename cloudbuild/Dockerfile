# This Dockerfile spins up a container where presubmit tests are run.
# Cloud Build orchestrates this process.

FROM gcr.io/cloud-builders/gcloud

RUN useradd -m -d /home/ia-tests -s /bin/bash ia-tests

COPY --chown=ia-tests:ia-tests . /init-actions

# Install Bazel:
# https://docs.bazel.build/versions/master/install-ubuntu.html
ENV bazel_kr_path=/usr/share/keyrings/bazel-keyring.gpg
RUN echo "deb [arch=amd64 signed-by=${bazel_kr_path}] http://storage.googleapis.com/bazel-apt stable jdk1.8" | tee /etc/apt/sources.list.d/bazel.list
RUN curl https://bazel.build/bazel-release.pub.gpg | gpg --dearmor -o "${bazel_kr_path}"
RUN apt-get update && apt-get install -y openjdk-8-jdk python3-setuptools bazel

USER ia-tests
