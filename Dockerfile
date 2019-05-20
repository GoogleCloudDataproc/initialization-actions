

FROM gcr.io/cloud-builders/gcloud

COPY main.sh /
COPY dummy_script.py /

RUN apt-get update
RUN apt-get -y install python
RUN apt-get -y install python3-pip

ENV PATH=$PATH:/builder/google-cloud-sdk/bin/

RUN git config --system credential.helper gcloud.sh

RUN pip install -r requirements.txt

ENTRYPOINT ["git"]