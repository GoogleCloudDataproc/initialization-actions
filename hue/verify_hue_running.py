"""verify_hue_running.py: Script for Hue initialization action test."""

import importlib
import logging
import socket
import subprocess
import time

BASE = 'localhost'
PORT = 8888
USERNAME = 'admin'
PASSWORD = 'admin'
WAREHOUSE = 'hdfs://' + socket.gethostname() + '/user/hive/warehouse'


class HueClient(object):
    """HueClient is a client to the Hue server.

    Attributes:
      base (str): The base url. Examples include localhost or 127.0.0.1.
      port (int): The port number Hue is listening on.
    """

    def __init__(self, base, port):
        self._base = 'http://{}:{}'.format(base, port)
        requests = self._import_module('requests')
        self._session = requests.Session()

    def _import_module(self, name):
        """Imports a module, and installs it first if needed."""
        try:
            module = importlib.import_module(name)
        except ImportError:
            pip = importlib.import_module('pip')
            pip.main(['install', name])
            module = importlib.import_module(name)
        return module

    def login(self, username, password):
        """Logs into Hue using the provided username and password.

        Based on example from Hue documentation:
        http://gethue.com/login-into-hue-using-the-python-request-library/

        Args:
          username (str): Hue username
          password (str): Hue password

        Raises:
          requests.exceptions.HTTPError: If an error occurred while logging in.
        """

        next_url = '/'
        full_path = '{}/accounts/login/?next={}'.format(self._base, next_url)

        response = self._session.get(full_path)
        response.raise_for_status()

        response = self._session.post(
            full_path,
            data={
                'username': username,
                'password': password,
                'csrfmiddlewaretoken': self._session.cookies['csrftoken'],
                'next': next_url
            },
            cookies={},
            headers={'Referer': full_path})
        response.raise_for_status()

    def check_metadata(self, wait_seconds=5, max_retries=20):
        """Verifies metadata about the default database.

        Returns:
          The response json.

        Args:
          wait_seconds (int): number of seconds to wait between request retries.
          max_retries (int): total number of time to retry request until success.

        Raises:
          requests.exceptions.HTTPError: If the metadata could not be retrieved.
          ValueError: If the value of the metadata was unexpected.
          RuntimeError: If the request is anything under status code 300.
        """

        # Retry a few times. Note that "errors" still come back as 2xx status codes,
        # but have a non-zero status field in the JSON body.
        for _ in range(0, max_retries):
            response = self._session.get(
                '{}/metastore/databases/default/metadata'.format(self._base),
                cookies=self._session.cookies,
                headers=self._session.headers)
            response.raise_for_status()

            metadata_json = response.json()
            print('Response:', metadata_json)
            # Successful response (metadata status is 0)
            if metadata_json['status'] == 0:
                if metadata_json['data']['location'] == WAREHOUSE:
                    return
                else:
                    raise ValueError('Unexpected location for default database.'
                                     'Expected: {}, Actual: {}'.format(
                        WAREHOUSE, metadata_json['data']['location']))

            time.sleep(wait_seconds)

        raise RuntimeError('Failed to check metadata: {}'.format(response.text))


def _make_hive_queries():
    """Execute sample hive queries in run_queries.py.

    Raises:
      ValueError: If the query results did not contain the expected values.
    """

    # Run some hive queries in the Hue shell
    cmd = '/usr/lib/hue/build/env/bin/hue shell < run_queries.py'
    p = subprocess.Popen(cmd, bufsize=4096, stdout=subprocess.PIPE, shell=True)
    out = p.communicate()[0].decode('utf-8')
    if out != "['a']\n['b']\n" and out != "[u'a']\n[u'b']\n":
        raise ValueError(
            'Hive query returned unexpected result.\nExpected: "{}"\nor "{}"'
            '\nActual: "{}"'.format("['a']\n['b']\n", "[u'a']\n[u'b']\n", out))


def main():
    """Drives the script.

    Returns:
      None

    Raises:
      Error: If a response does not contain the expected value.
    """

    # Enable DEBUG logging for requests library
    logging.basicConfig(level=logging.DEBUG)

    hue_client = HueClient(BASE, PORT)

    # Test logging in
    hue_client.login(USERNAME, PASSWORD)
    hue_client.check_metadata()

    # Test making some hive queries in the Hue shell
    _make_hive_queries()


if __name__ == '__main__':
    main()