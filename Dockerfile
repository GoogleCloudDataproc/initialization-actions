FROM debian
COPY main.sh /
COPY dummy_script.py /

RUN apt-get update
RUN apt-get -y install python

CMD ["python", "./dummy_script.py"]