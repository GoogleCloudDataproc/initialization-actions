# This Dockerfile spins up a container where presubmit tests are run.
# Cloud Build orchestrates this process.

FROM gcr.io/cloud-builders/gcloud

RUN ls -a

# Install Pip and dependencies
RUN apt-get -y update
RUN apt-get -y install python3-pip
RUN pip3 install -r integration_tests/requirements.txt

# Install fastunit
WORKDIR /tmp/fastunit/
RUN git clone https://github.com/ityoung/python3-fastunit.git .
RUN python3 setup.py install
