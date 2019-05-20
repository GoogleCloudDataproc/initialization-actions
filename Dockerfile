

FROM gcr.io/cloud-builders/gcloud

COPY main.sh /
COPY dummy_script.py /

RUN apt-get update
RUN apt-get -y install python

ENV PATH=$PATH:/builder/google-cloud-sdk/bin/

RUN git config --system credential.helper gcloud.sh

pip install -r requirements.txt

ENTRYPOINT ["git"]