FROM alpine
COPY main.sh /
COPY dummy_script.py /

RUN sudo apt-get update
RUN sudo apt-get install python

CMD ["python", "./dummy_script.py"]