FROM debian:jessie
RUN apt-get update
RUN apt-get install -y git python3  python3-pip curl gnupg2
RUN apt-get install -y cpio vim curl
# Set up gcloud.
ENV CLOUD_SDK_REPO=cloud-sdk-jessie
RUN echo "deb http://packages.cloud.google.com/apt $CLOUD_SDK_REPO main" > /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add -
RUN apt-get update
RUN apt-get install -y google-cloud-sdk
RUN gcloud config set core/disable_usage_reporting true
RUN gcloud config set component_manager/disable_update_check true
RUN gcloud config set compute/zone us-west1-a
RUN	gcloud config set compute/region us-west1
RUN mkdir /usr/local/test-scripts
COPY ./ /usr/local/dataproc-initialization-actions
WORKDIR /usr/local/dataproc-initialization-actions/testing-scripts
RUN pip3 install -r requirements.txt
