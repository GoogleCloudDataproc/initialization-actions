FROM alpine
COPY main.sh /
COPY dummy_script.py /

RUN apt-get update
RUN apt-get install python

CMD ["python", "./dummy_script.py"]