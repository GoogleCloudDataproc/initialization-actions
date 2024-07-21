"""verify_oozie_running.py: Script for Oozie initialization action test."""
import fileinput
import importlib
import os
import socket
import subprocess
import sys
import time

HOST = socket.gethostname()
PORT = 11000


class OozieClient(object):
    """OozieClient is a client to the Oozie REST API.

    It performs simple tasks like querying server/job status.
    For heavy lifting tasks like starting a new workflow, use
    Oozie command line tool.

    Attr:
      host (str): The host name.
      port (int): The port number Oozie server is listening on.
    """

    def __init__(self, host, port):
        self.base = 'http://{}:{}/oozie/v1'.format(host, port)

    def _import_module(self, name):
        """Imports a module, install it first if needed."""
        try:
            module = importlib.import_module(name)
        except ImportError:
            pip = importlib.import_module('pip')
            pip.main(['install', name])
            module = importlib.import_module(name)
        return module

    def _make_request(self,
                      verb,
                      path,
                      transform_func,
                      error_msg,
                      wait_seconds=5,
                      max_retries=20,
                      **kwargs):
        """Makes an HTTP request and fails if not successful.

        Args:
          verb (str): HTTP verb (GET, POST, PUT, etc).
          path (str): Path to make the request on.
          transform_func (func): A function that takes the response object and can
            extract what the user needs from it.
          error_msg: Message to include in the exception if raised.
          wait_seconds (int): number of seconds to wait between request retries.
          max_retries (int): total number of time to retry request until success.
          **kwargs: Any args accepted as kwargs from the requests module.

        Returns:
          The result of transform_func.

        Raises:
          Exception: If the request is anything under status code 300.
        """
        requests = self._import_module('requests')

        # Use constant backoff instead of expotentail backoff,
        # because we are waiting on service to come up
        for _ in range(0, max_retries):
            response = requests.request(verb, self.base + path, **kwargs)
            if response.status_code < 300:
                return transform_func(response)

            time.sleep(wait_seconds)

        raise Exception(error_msg + '\n' + response.text)

    def server_status(self):
        """Gets the status of the Oozie server."""

        print('Checking Oozie server status...')
        path = '/admin/status'
        status = self._make_request('GET', path,
                                    lambda response: response.json()['systemMode'],
                                    'Failed to check Oozie server status!')
        print('Oozie server status is: {}'.format(status))
        return status

    def job_status(self, job_id):
        """Gets the status of a workflow job."""

        print('Checking Oozie workflow job status for {}...'.format(job_id))
        path = '/job/{}'.format(job_id)
        data = {'show': 'info'}
        status = self._make_request(
            'GET',
            path,
            lambda response: response.json()['status'],
            'Failed to check job status for: {}'.format(job_id),
            json=data)
        print('Job status is: {}'.format(status))
        return status


def stage_example(job_type):
    """Stages the Oozie example to HDFS."""

    if not os.path.exists('/tmp/examples'):
        print('Unzipping Oozie examples...')
        unzip_cmd = 'tar -zxvf /usr/share/doc/oozie/oozie-examples.tar.gz -C /tmp'
        unzip_failure_message = 'Failed to unzip the oozie-examples tar!'
        run_command(unzip_cmd, unzip_failure_message)

    print('Rewriting {} job.properties file'.format(job_type))
    job_properties_file = '/tmp/examples/apps/{}/job.properties'.format(job_type)
    try:
        finput = fileinput.input(job_properties_file, inplace=True)
        for line in finput:
            if line.startswith('nameNode='):
                print('nameNode=hdfs://{}:8020'.format(HOST))
            elif line.startswith('resourceManager='):
                print('resourceManager={}:8032'.format(HOST))
            elif line.startswith('jobTracker='):
                print('jobTracker={}:8032'.format(HOST))
            elif line.startswith('jdbcURL='):
                print('jdbcURL=jdbc:hive2://{}:10000/default'.format(HOST))
            elif line.startswith('oozie.wf.application.path='):
                print('oozie.wf.application.path=${nameNode}/tmp/'
                      '${examplesRoot}/apps/%s/workflow.xml' % job_type)
            elif line:
                print(line)
    finally:
        finput.close()

    print('Rewriting {} workflow.xml file'.format(job_type))
    workflow_file = '/tmp/examples/apps/{}/workflow.xml'.format(job_type)
    try:
        finput = fileinput.input(workflow_file, inplace=True)
        for line in finput:
            print(line.replace('user/${wf:user()}', 'tmp').rstrip())
    finally:
        finput.close()

    print('Copying {} example to HDFS'.format(job_type))
    cp_cmd = ('hadoop dfsadmin -safemode leave;'
              'hadoop fs -put -f /tmp/examples /tmp/')
    cp_failure_message = 'Failed to copy {} example to HDFS!'.format(job_type)
    run_command(cp_cmd, cp_failure_message)


def run_example_job(job_type):
    """Run the example job on Oozie server being tested.

    Args:
      job_type: The type of the example job.

    Returns:
      The ID of the example job.

    Raises:
      RuntimeError: If the routine fails to change the job.properties
      file content.
      Exception: I
    """

    stage_example(job_type)

    print('Running example {} job...'.format(job_type))
    run_job_command = ('oozie job -oozie http://{}:11000/oozie '
                       '-config /tmp/examples/apps'
                       '/{}/job.properties -run'.format(HOST, job_type))
    run_job_failure_message = 'Failed to submit job!'
    output = run_command(run_job_command, run_job_failure_message).rstrip()
    job_id = output.split('job: ', 1)[1]
    return job_id


def run_command(command, failure_message):
    try:
        output = subprocess.check_output(command, shell=True)
        if isinstance(output, bytes):
            output = output.decode('utf-8')
        return output
    except subprocess.CalledProcessError:
        raise Exception(failure_message)


def check_job_status(oozie_client, job_id):
    """Checks job status.

    Args:
      oozie_client: Oozie client.
      job_id: job ID.

    Returns:
      True if the job has completed successfully, False if the job is running.

    Raises:
      Exception: if the job has failed or unable to determine the status.
    """

    job_status = oozie_client.job_status(job_id)
    print('Job {} status: {}'.format(job_id, job_status))
    if any(status in job_status for status in ['ERROR', 'FAILED', 'KILLED']):
        raise Exception('Job {} failed: {}'.format(job_id, job_status))
    if 'RUNNING' in job_status:
        return False
    if 'SUCCEEDED' in job_status:
        return True
    raise Exception('Unknown status for job {}: {}'.format(job_id, job_status))


def wait_for_job(oozie_client, job_id):
    for i in range(30):
        time.sleep(10)
        print('Checking job status {}'.format(i))
        if check_job_status(oozie_client, job_id):
            return
    raise Exception('Job {} timed out'.format(job_id))


def main():
    """Drives the script."""

    job_type = sys.argv[1]

    oozie_client = OozieClient(HOST, PORT)
    if oozie_client.server_status() != 'NORMAL':
        raise Exception('Oozie server is not running correctly!')

    job_id = run_example_job(job_type)
    wait_for_job(oozie_client, job_id)


if __name__ == '__main__':
    main()