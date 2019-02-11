"""verify_druid_running.py: Script for Druid initialization action test.
"""

import subprocess

DRUID_VERSION = '0.13.0-incubating'


def run_command(cmd):
    p = subprocess.Popen(
        cmd,
        shell=True,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    stdout, stderr = p.communicate()
    return stdout.decode("utf-8"), stderr.decode("utf-8")


class Druid(object):
    def __init__(self):
        self.druid_urls = ""
        self.druid_dir = "/opt/druid/apache-druid-{}".format(DRUID_VERSION)

        stdout, stderr = run_command("/usr/share/google/get_metadata_value attributes/druid-overlord-port")
        if stdout != "":
            self.druid_urls = "{}".format("--url http://localhost:{}/".format(stdout))

        stdout, stderr = run_command("/usr/share/google/get_metadata_value attributes/druid-coordinator-port")
        if stdout != "":
            self.druid_urls = "{} {}".format(self.druid_urls, "--coordinator-url http://localhost:{}/".format(stdout))

    def submit_task(self):
        cmd = "{}/bin/post-index-task  " \
              "--file {}/quickstart/tutorial/wikipedia-index.json {}".format(
            self.druid_dir, self.druid_dir, self.druid_urls)
        print(cmd)
        stdout, stderr = run_command(cmd)
        if stdout:
            print(stdout)
        if stderr:
            print(stderr)
            return stderr


def main():
    """Drives the script.

    Returns:
      None

    Raises:
      Exception: If a response does not contain the expected value
    """
    druid = Druid()
    print("Starting test for Druid")
    output = druid.submit_task()
    assert (output.find("Task finished with status: SUCCESS") > 0)
    assert (output.find("wikipedia loading complete! You may now query your data") > 0)
    print("Success")


if __name__ == '__main__':
    main()
