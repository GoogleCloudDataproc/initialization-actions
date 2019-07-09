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

USER ia-tests
