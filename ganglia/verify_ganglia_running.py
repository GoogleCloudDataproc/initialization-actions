"""verify_ganglia_running.py: Script for ganglia initialization action test.
"""
import socket
import subprocess

from requests_html import HTMLSession

BASE = 'localhost'
PORT = 80


class Ganglia(object):
    def __init__(self, base, port):
        self.path = 'http://{}:{}/ganglia/'.format(base, port)
        self.host = socket.gethostname()
        self.cluster_name = self.get_cluster_name()
        if self.host in self.get_main_master():
            self.is_main_master = True
        else:
            self.is_main_master = False

    def get_homepage_title(self):
        session = HTMLSession()
        r = session.get(self.path)
        try:
            return r.html.find('#page_title', first=True).text
        except Exception:
            return None

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
        return stdout.decode("utf-8")

    def get_cluster_name(self):
        cmd = '/usr/share/google/get_metadata_value attributes/dataproc-cluster-name'
        p = subprocess.Popen(
            cmd,
            shell=True,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        stdout, stderr = p.communicate()
        return stdout.decode("utf-8")


def validate_homepage(ganglia):
    if ganglia.is_main_master:
        if ganglia.cluster_name in ganglia.get_homepage_title():
            print('Ganglia UI is running on master node')
        else:
            raise Exception('Ganglia UI is not found on master node')
    else:
        if '-w-' in ganglia.host:
            print("Ganglia UI should not run on worker node")
        elif '-m-' in ganglia.host:
            print("Ganglia UI should not run on additional master")


def main():
    """Drives the script.

    Returns:
      None

    Raises:
      Exception: If a response does not contain the expected value
    """
    ganglia = Ganglia(BASE, PORT)
    validate_homepage(ganglia)


if __name__ == '__main__':
    main()
