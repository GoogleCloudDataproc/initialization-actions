# This Dockerfile spins up a container where presubmit tests are run.
# Cloud Build orchestrates this process.

FROM gcr.io/cloud-builders/gcloud

# Copy everything into the container
COPY cloud-build/requirements.pip /
RUN ls -a

RUN add-apt-repository ppa:jonathonf/python-3.6
RUN apt-get -y update
RUN apt-get -y install python3-pip
RUN apt-get install -y python3.6

RUN python3.5 -m pip install --upgrade pip
RUN python3.5 -m pip install -r cloud-build/requirements.pip

RUN pip3 freeze
RUN python3 -V
RUN python3.5 -V

ENV PATH=$PATH:/builder/google-cloud-sdk/bin/
ENV PYTHONPATH /

RUN pip3 install parameterized

RUN pip3 install -r cloud-build/requirements.pip

COPY . /
WORKDIR /

# Make sure to use dependency versions that work
# RUN pip freeze > cloud-build/requirements.pip

ENTRYPOINT ["bash"]
