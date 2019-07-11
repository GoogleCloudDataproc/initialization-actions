"""verify_livy_running.py: Script for livy initialization action test.
"""
import json
import requests
import textwrap
import time

WAIT_SECONDS = 100


class Livy:
    host = 'http://localhost:8998'
    session_url = None
    statements_url = None
    session_data = {'kind': 'spark'}
    headers = {'Content-Type': 'application/json'}

    def create_session(self):
        resp = requests.post(self.host + '/sessions',
                             data=json.dumps(self.session_data),
                             headers=self.headers)
        self.session_url = self.host + resp.headers['Location']

    def wait_for_session_idle(self):
        wait_seconds_remain = WAIT_SECONDS
        while wait_seconds_remain > 0:
            resp = requests.get(self.session_url, headers=self.headers)
            if resp.json()['state'] == 'idle':
                self.statements_url = self.session_url + '/statements'
                return

            time.sleep(5)
            wait_seconds_remain -= 5

        print('Failure - Spark session initialization to idle state timed out')
        exit(1)

    def submit_job(self, data):
        requests.post(self.statements_url,
                      data=json.dumps(data),
                      headers=self.headers)

    def validate_job_result(self, expected):
        wait_seconds_remain = WAIT_SECONDS
        while wait_seconds_remain > 0:
            resp = requests.get(self.statements_url, headers=self.headers)
            try:
                data = resp.json()['statements'][0]['output']['data']
                if data is not None and expected in data['text/plain']:
                    print("OK - Spark job succeeded")
                    return
            except (KeyError, TypeError):
                pass

            time.sleep(5)
            wait_seconds_remain -= 5

        print('Failure - Spark job execution timed out')
        exit(1)


def main():
    """Drives the script.

    Returns:
      None

    Raises:
      Exception: If a response does not contain the expected value
    """
    livy = Livy()
    livy.create_session()
    livy.wait_for_session_idle()
    code = textwrap.dedent("""
        val NUM_SAMPLES = 100000;
        val count = sc.parallelize(1 to NUM_SAMPLES).map { i =>
          val x = Math.random();
          val y = Math.random();
          if (x*x + y*y < 1) 1 else 0
        }.reduce(_ + _);
        println("Pi is roughly " + 4.0 * count / NUM_SAMPLES)
        """)
    data = {'code': code}

    livy.submit_job(data)
    livy.validate_job_result('Pi is roughly')


if __name__ == '__main__':
    main()
