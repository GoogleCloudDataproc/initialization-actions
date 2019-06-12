# This Dockerfile spins up a container where presubmit tests are run.
# Cloud Build orchestrates this process.

FROM gcr.io/cloud-builders/gcloud

RUN mkdir -p /usr/src/app
COPY cloud-build/requirements.pip /usr/src/app/

# Copy everything into the container
COPY cloud-build/requirements.pip /
RUN ls -a

RUN apt-get -y update
RUN apt-get -y install python3-pip
RUN apt-get install -y python3.6

RUN python3.5 -m pip install --upgrade pip
RUN python3.5 -m pip install -r /usr/src/app/requirements.pip # requirements.pip

RUN pip3 freeze
RUN python3 -V
RUN python3.5 -V

ENV PATH=$PATH:/builder/google-cloud-sdk/bin/
ENV PYTHONPATH /

COPY . /usr/src/app
WORKDIR /usr/src/app
# COPY . /
# WORKDIR /
RUN ls -a

# Make sure to use dependency versions that work
# RUN pip freeze > cloud-build/requirements.pip

ENTRYPOINT ["bash"]
