"""verify_hue_running.py: Script for Hue initialization action test.
"""

import requests
import socket
import subprocess
import sys
import time

BASE = 'localhost'
PORT = 8888


class Hue(object):
    def __init__(self, base, port):
        self.path = 'http://{}:{}/accounts/login/?next=/'.format(base, port)
        self.host = socket.gethostname()

    def get_response_code(self, retries=0):
        while True:
            try:
                response = requests.get(self.path)
                return response.status_code
            except requests.exceptions.RequestException as re:
                if retries > 0:
                    retries -= 1
                    time.sleep(3)
                    continue
                print("Failed to get Hue status code: {}".format(re),
                      file=sys.stderr)
                return None

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

    def is_main_master(self):
        if self.host == self.get_main_master().decode('utf-8'):
            return True
        else:
            return False


def main():
    """Drives the script.

    Returns:
      None

    Raises:
      Exception: If a response does not contain the expected value
    """
    hue = Hue(BASE, PORT)
    is_main_master = hue.is_main_master()
    if is_main_master:
        code = hue.get_response_code(retries=100)
        assert code is not None, "NOK - Failed to get a status code"
        assert code < 300, "NOK - Expected to see UI running on this node"
    else:
        code = hue.get_response_code()
        assert code is None, "NOK - Expected to not see UI running on this node"


if __name__ == '__main__':
    main()
