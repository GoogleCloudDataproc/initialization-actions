# This Dockerfile spins up a container where presubmit tests are run.
# Cloud Build orchestrates this process.

FROM gcr.io/cloud-builders/gcloud

# Copy everything into the container
COPY . init-actions/
WORKDIR init-actions/
RUN ls -a

RUN apt-get -y update && \
    apt-get -y install python3-pip

RUN python3.5 -m pip install --upgrade pip
RUN python3.5 -m pip install -r integration_tests/requirements.txt

ENV PATH=$PATH:/builder/google-cloud-sdk/bin/
ENV PYTHONPATH /

ENTRYPOINT ["bash"]
