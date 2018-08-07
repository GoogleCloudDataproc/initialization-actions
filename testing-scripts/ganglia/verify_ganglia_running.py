"""verify_ganglia_running.py: Script for Jupyter initialization action test.
"""

# This file was provided by Google.
# Added feature to pass expected_version as it's different on 1.0 1.1 and 1.2

import requests
import socket

BASE = 'localhost'
PORT = 80



class GangliaApi(object):
    def __init__(self, base, port):
        self.base = 'http://{}:{}'.format(base, port)
        self.host = socket.gethostname()

    def get_homepage(self):
        path = '/ganglia/'
        try:
            r = requests.get(self.base + path)
            html = r.text[:200]
            if "Cluster Report" in html:
                print("OK - Ganglia UI is running on master node")
            else:
                raise Exception('NOK - Could not find service UI running')
        except requests.exceptions.RequestException:
            if '-w-' in self.host:
                print("OK - Ganglia is not running on worker node")
            else:
                print("CONNECTION ERROR")

def main():
  """Drives the script.

  Returns:
    None

  Raises:
    Exception: If a response does not contain the expected value
  """
  gangliaApi = GangliaApi(BASE,PORT)
  res = gangliaApi.get_homepage()


if __name__ == '__main__':
  main()
