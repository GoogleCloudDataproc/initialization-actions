"""verify_knox_running.py: Script for knox initialization action test.
"""
import socket
import requests
import time

BASE = 'localhost'
PORT = 8443
FILENAME = "knox-{}".format(time.time())
TEST_FILE_CONTENT = "Sample data"


class Knox(object):
    def __init__(self, base, port):
        self.path = 'https://{}:{}/gateway/sandbox/webhdfs/v1/'.format(base, port)
        self.host = socket.gethostname()
        self.auth = ('guest', 'guest-password')

    def get_knox_listen_status(self):
        r = requests.get("{}{}".format(self.path, '?op=LISTSTATUS'), verify=False, auth=self.auth)
        try:
            return r.status_code
        except Exception:
            return None

    def create_file_in_hdfs(self):
        r = requests.put("{}{}".format(self.path, 'tmp/{}?op=CREATE'.format(FILENAME)), verify=False,
                         auth=self.auth, allow_redirects=False)
        try:
            return r.headers.get('Location')
        except Exception:
            return None

    def create_file_to_upload(self):
        fo = open(FILENAME, "w")
        fo.write(TEST_FILE_CONTENT)
        fo.close()

    def put_file_in_hdfs(self, location):
        r = requests.put(location, verify=False,
                         auth=self.auth, files={'file': open(FILENAME, 'rb')})
        try:
            return r
        except Exception:
            return None

    def open_file_in_hdfs(self):
        r = requests.get("{}{}".format(self.path, "tmp/{}?op=OPEN".format(FILENAME)), verify=False,
                         auth=self.auth, allow_redirects=False)
        try:
            return r.headers.get('Location')
        except Exception:
            return None

    def read_file_from_hdfs(self, location):
        r = requests.get(location, verify=False,
                         auth=self.auth)
        try:
            return r.text
        except Exception:
            return None


def main():
    """Drives the script.

    Returns:
      None
    Raises:
      Exception: If a response does not contain the expected value
    """
    knox = Knox(BASE, PORT)

    return_code = knox.get_knox_listen_status()
    assert (return_code == 200)

    knox.create_file_to_upload()
    location = knox.create_file_in_hdfs()

    knox.put_file_in_hdfs(location)
    location = knox.open_file_in_hdfs()

    text = knox.read_file_from_hdfs(location)
    assert (text.find(TEST_FILE_CONTENT) > 0)


if __name__ == '__main__':
    main()
