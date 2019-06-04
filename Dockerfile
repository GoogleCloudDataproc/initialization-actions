FROM gcr.io/cloud-builders/gcloud

# Copy everything into the container
COPY . /
COPY ./.git/ ./.git/
RUN ls -a

RUN apt-get -y update
RUN apt-get -y install python3-pip

ENV PATH=$PATH:/builder/google-cloud-sdk/bin/
ENV PYTHONPATH /

RUN pip install -r requirements.pip

# Make sure to use dependency versions that work
RUN pip freeze > requirements.pip

ENTRYPOINT ["bash"]
