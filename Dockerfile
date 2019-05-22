

FROM gcr.io/cloud-builders/gcloud

COPY . /
RUN ls


RUN apt-get -y update

RUN apt-get -y install python3
RUN apt-get -y install python3-pip

ENV PATH=$PATH:/builder/google-cloud-sdk/bin/
ENV PYTHONPATH /

RUN git config --system credential.helper gcloud.sh

RUN ls -l `which pip`
RUN ls -l `which pip3`
RUN pip install -r requirements.txt
RUN pip3 install -r requirements.txt
RUN pip freeze > requirements.txt

ENTRYPOINT ["git"]