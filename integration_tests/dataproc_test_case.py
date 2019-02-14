import os
import json
import logging
import datetime
import unittest
import subprocess
from threading import Timer

logging.basicConfig(level=os.getenv("LOG_LEVEL", logging.WARNING))

DEFAULT_TIMEOUT = 10  # minutes


class DataprocTestCase(unittest.TestCase):
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
    INIT_ACTION = None

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        assert cls.COMPONENT
        assert cls.INIT_ACTION

    def createCluster(self, configuration, init_action, dataproc_version,
                      metadata=None, scopes=None, properties=None,
                      timeout_in_minutes=None, beta=False,
                      master_accelerator=None, worker_accelerator=None):
        self.name = "test-{}-{}-{}-{}".format(
            self.COMPONENT,
            configuration.lower(),
            dataproc_version.replace(".", "-"),
            datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        )
        self.cluster_version = None

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
            args.append("--initialization-action-timeout {}m".format(timeout_in_minutes))
        args.append("--initialization-actions {}".format(init_action))
        if master_accelerator:
            args.append("--master-accelerator {}".format(master_accelerator))
        if worker_accelerator:
            args.append("--worker-accelerator {}".format(worker_accelerator))
        cmd = "gcloud dataproc clusters create {}".format(self.name)
        if beta:
            cmd = "gcloud beta dataproc clusters create {}".format(self.name)
        for flag in args:
            cmd += " {}".format(flag)
        cmd += " --format=json"

        ret_val, stdout, stderr = self.run_command(cmd, timeout_in_minutes=timeout_in_minutes or DEFAULT_TIMEOUT)

        self.assertEqual(ret_val, 0, "Failed to create Cluster {}. Error: {}".format(
            self.name,
            stderr
        ))
        self.cluster_version = json.loads(stdout).get("config", {}).get("softwareConfig", {}).get("imageVersion")

    def tearDown(self):
        cmd = "gcloud dataproc clusters delete {} --quiet".format(self.name)
        ret_val, stdout, stderr = self.run_command(cmd)
        self.assertEqual(ret_val, 0, "Failed to delete cluster {}. Error: {}".format(
            self.name,
            stderr
        ))

    def getClusterName(self):
        return self.name

    def upload_test_file(self, testfile, name):
        cmd = 'gcloud compute scp {} {}:'.format(
            testfile,
            name,
        )
        ret_code, stdout, stderr = self.run_command(cmd)
        self.assertEqual(ret_code, 0, "Failed to upload test file. Error: {}".format(stderr))

    def remove_test_script(self, testfile, name):
        ret_code, stdout, stderr = self.run_command(
            'gcloud compute ssh {} -- "rm {}"'.format(
                name,
                testfile
            )
        )
        self.assertEqual(ret_code, 0, "Failed to remove test file. Error: {}".format(stderr))

    @staticmethod
    def run_command(command, timeout_in_minutes=DEFAULT_TIMEOUT):
        kill = lambda process: process.kill()
        p = subprocess.Popen(
            command,
            shell=True,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        timeout = timeout_in_minutes * 60
        my_timer = Timer(timeout, kill, [p])
        try:
            my_timer.start()
            stdout, stderr = p.communicate()
            stdout, stderr = stdout.decode('utf-8'), stderr.decode('utf-8')
        finally:
            my_timer.cancel()
        logging.info("Ran %s: retcode: %d, stdout: %s, stderr: %s", command, p.returncode, stdout, stderr)
        return p.returncode, stdout, stderr

    @staticmethod
    def generate_verbose_test_name(testcase_func, param_num, param):
        return "{}.mode: {}.version: {}".format(
            testcase_func.__name__,
            param.args[0],
            param.args[1].replace(".", "_"),
        )


if __name__ == '__main__':
    unittest.main()
