"""verify_jupyter_running.py: Script for Jupyter initialization action test.
"""

# This file was provided by Google.
# Added feature to pass expected_version as it's different on 1.0 1.1 and 1.2

import sys
import requests

EXPECTED_VERSION = '5.0.0'
PYSPARK = 'pyspark'
PYTHON = 'python3'
EXPECTED_NAME = 'Untitled.ipynb'

BASE = 'localhost'
PORT = 8123

# must match token set in metadata in JupyterTest.java
JUPYTER_AUTH_TOKEN = 'abc123'


class JupyterApi(object):
  """JupyterApi is a client to the Jupyter Notebook Server API.

  Args:
    base (str): The base url. Examples include localhost or 127.0.0.1.
    port (int): The port number Jupyter is listening on.
  """

  def __init__(self, base, port):
    self.base = 'http://{}:{}/api'.format(base, port)

  def get_api_version(self):
    """Gets the version of the Jupyter Notebook Server API that is running.

    Returns:
      string: API version.
    """

    path = ''

    return self._make_request(
        'GET',
        path,
        lambda response: response.json()['version'],
        'Failed to get api version.')

  def get_kernels(self):
    """Retrieves the kernels with which Jupyter is configured to run.

    Returns:
      list: List of kernels with which Jupyter is configured to run.
    """

    path = '/kernelspecs'

    return self._make_request(
        'GET',
        path,
        lambda response: list(response.json()['kernelspecs'].keys()),
        'Failed to get api version.')

  def make_notebook(self):
    """Creates a new notebook.

    Returns:
      string: The name of the newly created notebook.
    """
    path = '/contents'
    data = {'type': 'notebook'}

    return self._make_request(
        'POST',
        path,
        lambda response: response.json()['name'],
        'Failed to create notebook.',
        json=data)

  def _make_request(self, verb, path, transform_func, error_msg, **kwargs):
    """Makes an HTTP request and fails if not successful.

    Args:
      verb (str): HTTP verb (GET, POST, PUT, etc).
      path (str): Path to make the request on.
      transform_func (func): A function that takes the response object and
      can extract what the user needs from it.

      error_msg: Message to include in the exception if raised.
      **kwargs: Any args accepted as kwargs from the requests module.

    Returns:
      The result of transform_func.

    Raises:
      Exception: If the request is anything under status code 300.
    """

    response = requests.request(verb, self.base + path +
                                '?token=' + JUPYTER_AUTH_TOKEN,
                                **kwargs)
    if not self._is_successful_response(response):
      raise Exception(error_msg + '\n' + response.text)
    return transform_func(response)

  def _is_successful_response(self, response):
    """Checks if the response had a successful call.

    Defines success as any status code under 300.

    Args:
      response (obj): The response object from the requests module.

    Returns:
      bool: True if the request is a 2xx or below. False otherwise.
    """
    return response.status_code < 300


def main(expected_version):
  """Drives the script.

  Returns:
    None

  Raises:
    Exception: If a response does not contain the expected value
  """

  jupyter_api = JupyterApi(BASE, PORT)

  # Test getting API version
  version = jupyter_api.get_api_version()
  if version != expected_version:
    raise Exception('Incorrect API version. Expected: {}, Actual: {}'.
                    format(expected_version, version))

  # Test to see if pyspark and python3 kernels configured
  kernels = jupyter_api.get_kernels()
  if PYSPARK not in kernels:
    raise Exception('Could not find expected kernel. Expected: {}, Actual: {}'.
                    format(PYSPARK, kernels))
  if PYTHON not in kernels:
    raise Exception('Could not find expected kernel. Expected: {}, Actual: {}'.
                    format(PYTHON, kernels))

  # Test creating a new ipython notebook
  notebook_name = jupyter_api.make_notebook()
  if notebook_name != EXPECTED_NAME:
    raise Exception(
        'Unexpected name for created notebook. Expected: {}, Actual: {}'.
        format(EXPECTED_NAME, notebook_name))


if __name__ == '__main__':
  if len(sys.argv) == 1:
    expected_version = EXPECTED_VERSION
  else:
    expected_version = sys.argv[1]
  main(expected_version)
