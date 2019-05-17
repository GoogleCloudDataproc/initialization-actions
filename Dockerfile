FROM debian
COPY main.sh /
COPY dummy_script.py /

ENTRYPOINT ["git"]