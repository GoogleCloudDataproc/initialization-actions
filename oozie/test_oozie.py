import os

import pkg_resources
from absl.testing import absltest
from absl.testing import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class OozieTestCase(DataprocTestCase):
    COMPONENT = 'oozie'
    INIT_ACTIONS = ['oozie/oozie.sh']
    TEST_SCRIPT_FILE_NAME = 'validate.sh'
    PYTHON2_VERSION = 'python2.7'
    PYTHON3_VERSION = 'python3'
    TEST_SCRIPT_JOB = 'verify_oozie_running.py'
    CONFIGURATION = 'STANDARD'

    def verify_instance(self, name):
        test_script_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            self.TEST_SCRIPT_FILE_NAME)
        self.upload_test_file(test_script_path, name)
        self.__run_test_file(name)
        self.remove_test_script(self.TEST_SCRIPT_FILE_NAME, name)

    def __run_test_file(self, name):
        self.assert_instance_command(
            name, "bash {}".format(self.TEST_SCRIPT_FILE_NAME))

    @parameterized.parameters(
        ("SINGLE", ["m"]),
        ("STANDARD", ["m"]),
        ("HA", ["m-0", "m-1", "m-2"]),
        ("KERBEROS", ["m"]),
    )
    def test_oozie(self, configuration, machine_suffixes):
        if self.getImageOs() == 'rocky':
            self.skipTest("Not supported in Rocky Linux-based images")

        self.createCluster(
            configuration,
            self.INIT_ACTIONS,
            machine_type="e2-standard-4",
            boot_disk_size="200GB")
        for machine_suffix in machine_suffixes:
            self.verify_instance("{}-{}".format(self.getClusterName(),
                                                machine_suffix))

    def __submit_pyspark_job(self, configuration, job_args):
        if self.getImageOs() == 'rocky':
            self.skipTest("Not supported in Rocky Linux-based images")

        if self.getImageVersion() >= pkg_resources.parse_version("2.1"):
            python_version = self.PYTHON3_VERSION
        else:
            python_version = self.PYTHON2_VERSION

        self.createCluster(
            configuration,
            self.INIT_ACTIONS,
            machine_type="e2-standard-4",
            boot_disk_size="200GB"
        )
        self.assert_dataproc_job(self.getClusterName(), 'pyspark',
                                 '{}/{}/{} --properties=spark.pyspark.python={},spark.pyspark.driver.python={}'
                                 ' -- {}'
                                 .format(self.INIT_ACTIONS_REPO,
                                         self.COMPONENT,
                                         self.TEST_SCRIPT_JOB,
                                         python_version,
                                         python_version,
                                         job_args))

    def test_distcp_job(self):
        self.__submit_pyspark_job(self.CONFIGURATION, "distcp")

    def test_hive_job(self):
        self.__submit_pyspark_job(self.CONFIGURATION, "hive2")

    def test_map_reduce_job(self):
        self.__submit_pyspark_job(self.CONFIGURATION, "map-reduce")

    def test_pig_job(self):
        if self.getImageVersion() <= pkg_resources.parse_version("1.5"):
            self.skipTest("Skip 1.5 images as Pig test fails there")
        self.__submit_pyspark_job(self.CONFIGURATION, "pig")

    def test_shell_job(self):
        self.__submit_pyspark_job(self.CONFIGURATION, "shell")

    def test_spark_job(self):
        if self.getImageVersion() > pkg_resources.parse_version("1.5"):
            self.__submit_pyspark_job(self.CONFIGURATION, "spark")
        else:
            self.skipTest("Skip 1.5 images as Spark test fails there")

    def test_sqoop_job(self):
        self.__submit_pyspark_job(self.CONFIGURATION, "sqoop")


if __name__ == '__main__':
    absltest.main()
