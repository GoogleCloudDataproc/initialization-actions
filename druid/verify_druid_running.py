"""verify_druid_running.py: Script for Druid initialization action test.
"""
import socket
import json
import requests
import time
import wget
import tarfile

BASE = 'localhost'
PORT = 8090
DRUID_VERSION = '0.12.3'


class Druid(object):
    def __init__(self, base, port):
        self.path = 'http://{}:{}/console.html'.format(base, port)
        self.host = socket.gethostname()

    def prepare_test_data(self):
        wget.download("http://druid.io/docs/{}/tutorials/tutorial-examples.tar.gz".format(DRUID_VERSION))
        tar = tarfile.open("tutorial-examples.tar.gz")
        tar.extractall()
        tar.close()

    def submit_and_get_task_id(self):
        with open('examples/wikipedia-index.json'.format(DRUID_VERSION)) as fp:
            obj = json.load(fp)
            r = requests.post("http://{}:{}/druid/indexer/v1/task".format(BASE, PORT),
                              headers={'Content-type': 'application/json'}, json=obj)
            return (r.json()["task"])

    def get_task_status(self, id):
        while True:
            r = requests.get("http://{}:{}/druid/indexer/v1/task/{}/status".format(BASE, PORT, id),
                             headers={'Content-type': 'application/json'})
            response = r.json()
            response = response.get("status")
            if response.get("duration") != -1:
                return response.get("status")
            print("Current task is still {}".format(response.get("status")))
            time.sleep(5)


def main():
    """Drives the script.

    Returns:
      None

    Raises:
      Exception: If a response does not contain the expected value
    """
    druid = Druid(BASE, PORT)
    druid.prepare_test_data()
    id = druid.submit_and_get_task_id()
    status = druid.get_task_status(id)
    assert (status, "SUCCESS", "Loading data into Druid failed - got {} status".format(status))



if __name__ == '__main__':
    main()
