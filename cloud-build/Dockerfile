# This Dockerfile spins up a container where presubmit tests are run.
# Cloud Build orchestrates this process.

FROM gcr.io/cloud-builders/gcloud

# Copy everything into the container
COPY . init-actions/

# Get fastunit dependency
RUN git clone https://github.com/ityoung/python3-fastunit.git
RUN cp -r python3-fastunit/fastunit init-actions/
RUN cp -r python3-fastunit/setup.py init-actions/
WORKDIR init-actions/
RUN ls -a

RUN apt-get -y update && \
    apt-get -y install python3-pip

RUN python3 -m pip install --upgrade pip
RUN python3 -m pip install -r integration_tests/requirements.txt

RUN python3 setup.py install

ENV PATH=$PATH:/builder/google-cloud-sdk/bin/
ENV PYTHONPATH /

ENTRYPOINT ["bash"]
