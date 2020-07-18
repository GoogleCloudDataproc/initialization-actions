import datetime
import json
import logging
import os
import random
import re
import string
import subprocess
import sys
from threading import Timer

import pkg_resources
from absl import flags
from absl.testing import parameterized

logging.basicConfig(level=os.getenv("LOG_LEVEL", logging.INFO))

FLAGS = flags.FLAGS
flags.DEFINE_string('image', None, 'Dataproc image URL')
flags.DEFINE_string('image_version', None, 'Dataproc image version, e.g. 1.4')
flags.DEFINE_boolean('skip_cleanup', False, 'Skip cleanup of test resources')
FLAGS(sys.argv)

INTERNAL_IP_SSH = os.getenv("INTERNAL_IP_SSH", "false").lower() == "true"

DEFAULT_TIMEOUT = 15  # minutes


class DataprocTestCase(parameterized.TestCase):
    DEFAULT_ARGS = {
        "SINGLE": [
            "--single-node",
        ],
        "STANDARD": [
            "--num-masters=1",
            "--num-workers=2",
        ],
        "HA": [
            "--num-masters=3",
            "--num-workers=2",
        ]
    }

    PROJECT = None
    REGION = None
    ZONE = None

    COMPONENT = None
    INIT_ACTIONS = None
    INIT_ACTIONS_REPO = None

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        _, project, _ = cls.run_command("gcloud config get-value project")
        cls.PROJECT = project.strip()

        _, zone, _ = cls.run_command("gcloud config get-value compute/zone")
        cls.ZONE = zone.strip()
        cls.REGION = cls.ZONE[:-2]

        assert cls.PROJECT
        assert cls.REGION
        assert cls.ZONE

        cls.INIT_ACTIONS_REPO = DataprocTestCase().stage_init_actions(
            cls.PROJECT)

        assert cls.COMPONENT
        assert cls.INIT_ACTIONS
        assert cls.INIT_ACTIONS_REPO

    def __init__(self, *args, **kwargs):
        super(parameterized.TestCase, self).__init__(*args, **kwargs)
        self.name = None

    def initClusterName(self, configuration):
        if self.name:
            return
        self.name = "test-{}-{}-{}-{}".format(
            self.COMPONENT, configuration.lower(),
            str(self.getImageVersion()).replace(".", "-"),
            self.datetime_str())[:46]
        self.name += "-{}".format(self.random_str(size=4))

    def createCluster(self,
                      configuration,
                      init_actions,
                      metadata=None,
                      scopes=None,
                      properties=None,
                      timeout_in_minutes=None,
                      beta=False,
                      master_accelerator=None,
                      worker_accelerator=None,
                      optional_components=None,
                      machine_type="e2-standard-2",
                      boot_disk_size="50GB"):
        self.initClusterName(configuration)
        self.cluster_version = None

        init_actions = [
            "{}/{}".format(self.INIT_ACTIONS_REPO, i)
            for i in init_actions or []
        ]

        args = self.DEFAULT_ARGS[configuration].copy()
        if FLAGS.image:
            args.append("--image={}".format(FLAGS.image))
        if FLAGS.image_version:
            args.append("--image-version={}".format(FLAGS.image_version))

        if optional_components:
            args.append("--optional-components={}".format(
                ','.join(optional_components)))

        if init_actions:
            args.append("--initialization-actions='{}'".format(
                ','.join(init_actions)))
        if timeout_in_minutes:
            args.append("--initialization-action-timeout={}m".format(
                timeout_in_minutes))

        if properties:
            args.append("--properties={}".format(properties))
        if metadata:
            args.append("--metadata={}".format(metadata))

        if scopes:
            args.append("--scopes={}".format(scopes))

        if master_accelerator:
            args.append("--master-accelerator={}".format(master_accelerator))
        if worker_accelerator:
            args.append("--worker-accelerator={}".format(worker_accelerator))

        args.append("--master-machine-type={}".format(machine_type))
        args.append("--worker-machine-type={}".format(machine_type))

        args.append("--master-boot-disk-size={}".format(boot_disk_size))
        args.append("--worker-boot-disk-size={}".format(boot_disk_size))

        args.append("--format=json")

        args.append("--region={}".format(self.REGION))

        cmd = "{} dataproc clusters create {} {}".format(
            "gcloud beta" if beta else "gcloud", self.name, " ".join(args))

        _, stdout, _ = self.assert_command(
            cmd, timeout_in_minutes=timeout_in_minutes or DEFAULT_TIMEOUT)
        self.cluster_version = json.loads(stdout).get("config", {}).get(
            "softwareConfig", {}).get("imageVersion")

    def stage_init_actions(self, project):
        bucket = "gs://dataproc-init-actions-test-{}".format(
            re.sub("[.:]", "", project.replace("google", "goog")))

        ret_val, _, _ = self.run_command("gsutil ls -b {}".format(bucket))
        # Create staging bucket if it does not exist
        if ret_val != 0:
            self.assert_command("gsutil mb {}".format(bucket))

        staging_dir = "{}/{}-{}".format(bucket, self.datetime_str(),
                                        self.random_str())

        self.assert_command(
            "gsutil -q -m rsync -r -x '.git*|.idea*' ./ {}/".format(
                staging_dir))

        return staging_dir

    def tearDown(self):
        try:
            self.name
        except AttributeError:
            logging.warning("Skipping cluster delete: name undefined")
            return

        if self.name is None:
            logging.warning("Skipping cluster delete: name is None")
            return

        if FLAGS.skip_cleanup:
            logging.warning(
                "Skipping cleanup because 'skip_cleanup' was"
                " specified! Please manually delete '%s' cluster"
                " when you have finished inspecting it.", self.name)
            return

        ret_code, _, stderr = self.run_command(
            "gcloud dataproc clusters delete {} --region={} --quiet --async".
            format(self.name, self.REGION))
        if ret_code != 0:
            logging.warning("Failed to delete '%s' cluster:\n%s", self.name,
                            stderr)

    def getClusterName(self):
        return self.name

    @staticmethod
    def getImageVersion():
        # Get a numeric version from the version flag: '1.5-debian10' -> '1.5'.
        # Special case a 'preview' image versions and return a large number
        # instead to make it a higher image version in comparisons
        version = FLAGS.image_version
        return pkg_resources.parse_version('999') if version.startswith(
            'preview') else pkg_resources.parse_version(version.split('-')[0])

    def upload_test_file(self, testfile, name):
        self.assert_command('gcloud compute scp {} {}:'.format(testfile, name))

    def remove_test_script(self, testfile, name):
        self.assert_instance_command(name, "rm {}".format(testfile))

    def assert_instance_command(self,
                                instance,
                                cmd,
                                timeout_in_minutes=DEFAULT_TIMEOUT):
        """Executes a command on VM instance and asserts that it returned 0 exit
        code.

        Args:
            instance: VM instance name to execute command on
            cmd: the command to execute
            timeout_in_minutes: timeout in minutes after which process that
                                executes command will be killed if it did not
                                finish
        Returns:
            ret_code: the return code of the command
            stdout: standard output of the command
            stderr: error output of the command
        Raises:
            AssertionError: if command returned non-0 exit code.
        """

        ret_code, stdout, stderr = self.assert_command(
            'gcloud compute ssh {} --command="{}"'.format(instance, cmd),
            timeout_in_minutes)
        return ret_code, stdout, stderr

    def assert_dataproc_job(self,
                            cluster_name,
                            job_type,
                            job_params,
                            timeout_in_minutes=DEFAULT_TIMEOUT):
        """Executes Dataproc job on a cluster and asserts that it returned 0
        exit code.

        Args:
            cluster_name: cluster name to submit job to
            job_type: job type (hadoop, spark, etc)
            job_params: job command parameters
            timeout_in_minutes: timeout in minutes after which process that
                                waits on job will be killed if job did not
                                finish
        Returns:
            ret_code: the return code of the job
            stdout: standard output of the job
            stderr: error output of the job
        Raises:
            AssertionError: if job returned non-0 exit code.
        """

        ret_code, stdout, stderr = self.assert_command(
            'gcloud dataproc jobs submit {} --cluster={} --region={} {}'.
            format(job_type, cluster_name, self.REGION,
                   job_params), timeout_in_minutes)
        return ret_code, stdout, stderr

    def assert_command(self, cmd, timeout_in_minutes=DEFAULT_TIMEOUT):
        """Executes a command locally and asserts that it returned 0 exit code.

        Args:
            cmd: the command to execute
            timeout_in_minutes: timeout in minutes after which process that
                                executes command will be killed if it did not
                                finish
        Returns:
            ret_code: the return code of the command
            stdout:
            stderr:
        Raises:
            AssertionError: if command returned non-0 exit code.
        """

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
    def run_command(cmd, timeout_in_minutes=DEFAULT_TIMEOUT):
        cmd = cmd.replace(
            "gcloud compute ssh ", "gcloud compute ssh --internal-ip ") if (
                INTERNAL_IP_SSH and "gcloud compute ssh " in cmd) else cmd
        cmd = cmd.replace("gcloud compute scp ",
                          "gcloud beta compute scp --internal-ip ") if (
                              INTERNAL_IP_SSH
                              and "gcloud compute scp " in cmd) else cmd
        p = subprocess.Popen(
            cmd,
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
        logging.debug("Ran %s: retcode: %d, stdout: %s, stderr: %s", cmd,
                      p.returncode, stdout, stderr)
        return p.returncode, stdout, stderr
