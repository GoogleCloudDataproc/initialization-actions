"""verify_hue_running.py: Script for Hue initialization action test.
"""

import requests
import socket
import subprocess

BASE = 'localhost'
PORT = 8888


class HueApi(object):
    def __init__(self, base, port):
        self.path = 'http://{}:{}/accounts/login/?next=/'.format(base, port)
        self.host = socket.gethostname()
        self.response_code = None
        self.is_main_master = None

    def get_reponse_code(self):
        try:
            response = requests.get(self.path)
            self.response_code = response.status_code
        except requests.exceptions.RequestException:
            self.response_code = None

    def validate_response_code(self):
        if self.is_main_master:
            if self.response_code:
                if self.response_code < 300:
                    print('OK - Hue UI is running on master node')
                else:
                    raise Exception('NOK - Could not find service UI running')
            else:
                raise Exception('NOK - Could not find service UI running')
        else:
            if '-w-' in self.host:
                if self.response_code:
                    if self.response_code > 300:
                        print('OK - Hue is not running on worker node')
                    else:
                        raise Exception('NOK - UI should not be found on worker node')
                else:
                    print('OK - Hue is not running on worker node')
            if '-m-' in self.host:
                if self.response_code:
                    if self.response_code > 300:
                        print('OK - Hue is not designed to be running on additional master nodes')
                    else:
                        raise Exception('NOK - UI should not be found on additional master node')
                else:
                    print('OK - Hue is not designed to be running on additional master nodes')

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

    def detect_role(self):
        if self.host == self.get_main_master():
            self.is_main_master = True
        else:
            self.is_main_master = False


def main():
    """Drives the script.

    Returns:
      None

    Raises:
      Exception: If a response does not contain the expected value
    """
    hueApi = HueApi(BASE, PORT)
    hueApi.detect_role()
    hueApi.get_reponse_code()
    hueApi.validate_response_code()


if __name__ == '__main__':
    main()
