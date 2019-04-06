"""verify_livy_running.py: Script for livy initialization action test.
"""
import json, requests, textwrap, time

DEFAULT_TIMEOUT = 60


class Livy:
    host = 'http://localhost:8998'
    session_url = ''
    statements_url = ''
    session_data = {'kind': 'spark'}
    headers = {'Content-Type': 'application/json'}

    def create_session(self):
        timeout = DEFAULT_TIMEOUT
        r = requests.post(self.host + '/sessions', data=json.dumps(self.session_data), headers=self.headers)
        while True:
            try:
                location = r.headers['Location']
                break
            except KeyError:
                None
            if timeout is 0:
                print('ERROR during spark session init')
                exit(1)
            time.sleep(5)
            timeout = timeout - 5
        self.session_url = self.host + location

    def wait_for_session_idle(self):
        timeout = DEFAULT_TIMEOUT
        while True:
            r = requests.get(self.session_url, headers=self.headers)
            if r.json()['state'] == 'idle':
                break
            if timeout is 0:
                print('Time has left - ERROR during spark session init')
                exit(1)
            time.sleep(5)
            timeout = timeout - 5
        self.statements_url = self.session_url + '/statements'

    def submit_job(self, data):
        requests.post(self.statements_url, data=json.dumps(data), headers=self.headers)

    def validate_job_result(self, expected):
        timeout = DEFAULT_TIMEOUT
        while True:
            try:
                r = requests.get(self.statements_url, headers=self.headers)
                if r.json()['statements'][0]['output']['data'] is not None:
                    if expected in r.json()['statements'][0]['output']['data']['text/plain']:
                        print("OK - Result of equation is found")
                        break
            except (KeyError, TypeError):
                time.sleep(5)
                timeout = timeout - 5

            if timeout is 0:
                print('ERROR during execution')
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
    data = {
        'code': textwrap.dedent("""
        val NUM_SAMPLES = 100000;
        val count = sc.parallelize(1 to NUM_SAMPLES).map { i =>
          val x = Math.random();
          val y = Math.random();
          if (x*x + y*y < 1) 1 else 0
        }.reduce(_ + _);
        println(\"Pi is roughly \" + 4.0 * count / NUM_SAMPLES)
        """)
    }

    livy.submit_job(data)
    livy.validate_job_result('Pi is roughly')


if __name__ == '__main__':
    main()
