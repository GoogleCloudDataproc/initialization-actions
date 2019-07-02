import os
import re
import sys
import json
import random
import string
import logging
import datetime
import unittest
import subprocess
from threading import Timer

BASE_TEST_CASE = unittest.TestCase
PARALLEL_RUN = False
if "fastunit" in sys.modules:
    import fastunit
    BASE_TEST_CASE = fastunit.TestCase
    PARALLEL_RUN = True

logging.basicConfig(level=os.getenv("LOG_LEVEL", logging.WARNING))

DEFAULT_TIMEOUT = 15  # minutes


class DataprocTestCase(BASE_TEST_CASE):
    DEFAULT_ARGS = {
        "SINGLE": [
            "--single-node",
        ],
        "STANDARD": [
            "--num-masters 1",
            "--num-workers 2",
        ],
        "HA": [
            "--num-masters 3",
            "--num-workers 2",
        ]
    }

    COMPONENT = None
    INIT_ACTIONS = None
    INIT_ACTIONS_REPO = None

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        cls.INIT_ACTIONS_REPO = DataprocTestCase().stage_init_actions()

        assert cls.COMPONENT
        assert cls.INIT_ACTIONS
        assert cls.INIT_ACTIONS_REPO

    def createCluster(self,
                      configuration,
                      init_actions,
                      dataproc_version,
                      metadata=None,
                      scopes=None,
                      properties=None,
                      timeout_in_minutes=None,
                      beta=False,
                      master_accelerator=None,
                      worker_accelerator=None,
                      optional_components=None):
        self.name = "test-{}-{}-{}-{}".format(
            self.COMPONENT, configuration.lower(),
            dataproc_version.replace(".", "-"), self.datetime_str())[:46]
        self.name += "-{}".format(self.random_str(size=4))
        self.cluster_version = None

        init_actions = [
            "{}/{}".format(self.INIT_ACTIONS_REPO, i)
            for i in init_actions or []
        ]

        args = self.DEFAULT_ARGS[configuration].copy()
        if properties:
            args.append("--properties={}".format(properties))
        if scopes:
            args.append("--scopes {}".format(scopes))
        if metadata:
            args.append("--metadata={}".format(metadata))
        if dataproc_version:
            args.append("--image-version={}".format(dataproc_version))
        if timeout_in_minutes:
            args.append("--initialization-action-timeout {}m".format(
                timeout_in_minutes))
        if init_actions:
            args.append("--initialization-actions '{}'".format(
                ','.join(init_actions)))
        if master_accelerator:
            args.append("--master-accelerator {}".format(master_accelerator))
        if worker_accelerator:
            args.append("--worker-accelerator {}".format(worker_accelerator))
        if optional_components:
            args.append("--optional-components {}".format(optional_components))
        cmd = "gcloud dataproc clusters create {}".format(self.name)
        if beta:
            cmd = "gcloud beta dataproc clusters create {}".format(self.name)
        for flag in args:
            cmd += " {}".format(flag)
        cmd += " --format=json"

        _, stdout, _ = self.run_and_assert_command(
            cmd, timeout_in_minutes=timeout_in_minutes or DEFAULT_TIMEOUT)
        self.cluster_version = json.loads(stdout).get("config", {}).get(
            "softwareConfig", {}).get("imageVersion")

    def stage_init_actions(self):
        _, project, _ = self.run_command("gcloud config get-value project")
        bucket = "gs://dataproc-init-actions-test-{}".format(
            re.sub("[.:]", "",
                   project.strip().replace("google", "goog")))

        ret_val, _, _ = self.run_command("gsutil ls -b {}".format(bucket))
        # Create staging bucket if it does not exist
        if ret_val != 0:
            self.run_and_assert_command("gsutil mb {}".format(bucket))

        staging_dir = "{}/{}-{}".format(bucket, self.datetime_str(),
                                        self.random_str())

        self.run_and_assert_command(
            "gsutil -q -m rsync -r -x '.git*|.idea*' ./ {}/".format(
                staging_dir))

        return staging_dir

    def tearDown(self):
        self.run_and_assert_command(
            "gcloud dataproc clusters delete {} --quiet".format(self.name))

    def getClusterName(self):
        return self.name

    def upload_test_file(self, testfile, name):
        self.run_and_assert_command('gcloud compute scp {} {}:'.format(
            testfile, name))

    def remove_test_script(self, testfile, name):
        self.run_and_assert_command(
            'gcloud compute ssh {} --command="rm {}"'.format(name, testfile))

    def run_and_assert_command(self, cmd, timeout_in_minutes=DEFAULT_TIMEOUT):
        ret_code, stdout, stderr = DataprocTestCase.run_command(
            cmd, timeout_in_minutes)
        self.assertEqual(
            ret_code, 0,
            "Failed to execute command:\n{}\nSTDOUT:\n{}\nSTDERR:\n{}".format(
                cmd, stdout, stderr))
        return ret_code, stdout, stderr

    @staticmethod
    def datetime_str():
        return datetime.datetime.now().strftime("%Y%m%d-%H%M%S")

    @staticmethod
    def random_str(size=4, chars=string.ascii_lowercase + string.digits):
        return ''.join(random.choice(chars) for _ in range(size))

    @staticmethod
    def run_command(command, timeout_in_minutes=DEFAULT_TIMEOUT):
        p = subprocess.Popen(
            command,
            shell=True,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        timeout = timeout_in_minutes * 60
        my_timer = Timer(timeout, lambda process: process.kill(), [p])
        try:
            my_timer.start()
            stdout, stderr = p.communicate()
            stdout, stderr = stdout.decode('utf-8'), stderr.decode('utf-8')
        finally:
            my_timer.cancel()
        logging.info("Ran %s: retcode: %d, stdout: %s, stderr: %s", command,
                     p.returncode, stdout, stderr)
        return p.returncode, stdout, stderr

    @staticmethod
    def generate_verbose_test_name(testcase_func, param_num, param):
        return "{} [mode={}, version={}, random_prefix={}]".format(
            testcase_func.__name__, param.args[0],
            param.args[1].replace(".", "_"), DataprocTestCase.random_str())


if __name__ == '__main__':
    if PARALLEL_RUN:
        fastunit.main()
    else:
        unittest.main()
