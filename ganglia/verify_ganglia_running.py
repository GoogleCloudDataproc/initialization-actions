"""verify_ganglia_running.py: Script for ganglia initialization action test.
"""
import requests
import socket
import subprocess

BASE = 'localhost'
PORT = 80


class GangliaApi(object):
    def __init__(self, base, port):
        self.path = 'http://{}:{}/ganglia/'.format(base, port)
        self.host = socket.gethostname()
        self.page = None
        self.is_main_master = None

    def validate_homepage(self):
        if self.page:
            if '-w-' in self.host:
                raise Exception('NOK - Ganglia should not be running on worker node')
            elif self.is_main_master:
                if 'Cluster Report' in self.page:
                    print('OK - Ganglia UI is running on master node')
                else:
                    raise Exception('NOK - Ganglia UI is not found on master node')
        else:
            if '-w-' in self.host:
                print('OK - Ganglia is not running on worker node')
            if '-m-' in self.host and not (self.is_main_master):
                print('OK - Ganglia is not designed to be running on additional master nodes')

    def get_homepage(self):
        try:
            self.page = requests.get(self.path).text[:200]
        except requests.exceptions.RequestException:
            self.page = None

    def detect_role(self):
        if self.host == self.get_main_master():
            self.is_main_master = True
        else:
            self.is_main_master = False

    def get_main_master(self):
        cmd = '/usr/share/google/get_metadata_value attributes/dataproc-master'
        p = subprocess.Popen(
            cmd,
            shell=True,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        stdout, stderr = p.communicate()
        return stdout


def main():
    """Drives the script.

    Returns:
      None

    Raises:
      Exception: If a response does not contain the expected value
    """
    gangliaApi = GangliaApi(BASE, PORT)
    gangliaApi.detect_role()
    gangliaApi.get_homepage()
    gangliaApi.validate_homepage()


if __name__ == '__main__':
    main()
