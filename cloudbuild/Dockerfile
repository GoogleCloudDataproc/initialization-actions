# This Dockerfile spins up a container where presubmit tests are run.
# Cloud Build orchestrates this process.

FROM gcr.io/cloud-builders/gcloud

RUN useradd -m -d /home/ia-tests -s /bin/bash ia-tests

COPY --chown=ia-tests:ia-tests . /init-actions

# Install Pip
RUN apt-get -y update
RUN apt-get -y install python3-pip

# Install fastunit
WORKDIR /tmp/fastunit
RUN git clone https://github.com/ityoung/python3-fastunit.git .
RUN python3 setup.py install

# Install Bazel (https://docs.bazel.build/versions/master/install-ubuntu.html)
RUN \
	apt-get -y install openjdk-8-jdk && \
    echo "deb [arch=amd64] http://storage.googleapis.com/bazel-apt stable jdk1.8" | tee /etc/apt/sources.list.d/bazel.list && \
    curl https://bazel.build/bazel-release.pub.gpg | apt-key add - && \
    apt-get update && \

    apt-get -y install bazel && \
    apt-get -y upgrade bazel

USER ia-tests
