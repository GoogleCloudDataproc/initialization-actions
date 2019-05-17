
FROM gcr.io/cloud-builders/git

COPY script.py /

RUN apt-get update
RUN apt-get -y install python

ENTRYPOINT ["python", "/script.py"]