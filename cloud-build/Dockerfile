# This Dockerfile spins up a container where presubmit tests are run.
# Cloud Build orchestrates this process.

FROM gcr.io/cloud-builders/gcloud

# Copy everything into the container
COPY . /
RUN ls -a

RUN apt-get -y update
RUN apt-get -y install python3.6
RUN apt-get -y install python3-pip

ENV PATH=$PATH:/builder/google-cloud-sdk/bin/
ENV PYTHONPATH /

RUN python3.6 -m pip install --upgrade pip
RUN python3.6 -m pip install -r cloud-build/requirements.pip

# RUN pip3 install -r cloud-build/requirements.pip

# Make sure to use dependency versions that work
# RUN pip freeze > cloud-build/requirements.pip

ENTRYPOINT ["bash"]
