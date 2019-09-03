"""This module provides testing functionality of the H2O Sparkling Water Init Action.
"""
import unittest
import os

from parameterized import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class H2OTestCase(DataprocTestCase):
    COMPONENT = "h2o"
    INIT_ACTIONS = ["h2o/h2o-dataproc-install-conda_pip.sh", "h2o/h2o-dataproc-tune.sh"]
    TEST_SCRIPT_FILE_NAME = "verify_h2o.py"
    SAMPLE_H2O_JOB_FILE_NAME = "sample-script.py"

    def verify_instance(self, name):
        self.upload_test_file(
            os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         self.TEST_SCRIPT_FILE_NAME), name)
        self.upload_test_file(
            os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         self.SAMPLE_H2O_JOB_FILE_NAME), name)
        self.__run_test_script(name)
        self.remove_test_script(self.TEST_SCRIPT_FILE_NAME, name)
        self.remove_test_script(self.SAMPLE_H2O_JOB_FILE_NAME, name)

    def __run_test_script(self, name):
        self.assert_instance_command(
            name, "python {} {} {}".format(self.TEST_SCRIPT_FILE_NAME, self.getClusterName(), self.SAMPLE_H2O_JOB_FILE_NAME))

    @parameterized.expand(
        [
            ("SINGLE", "1.4", ["m"]),
            ("STANDARD", "1.4", ["m"]),
            ("HA", "1.4", ["m-0"]),
        ],
        testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_h2o(self, configuration, dataproc_version, machine_suffixes):
        self.createCluster(
            configuration,
            self.INIT_ACTIONS,
            dataproc_version,
            machine_type="n1-standard-8",
            optional_components="ANACONDA",
            scopes="https://www.googleapis.com/auth/cloud-platform")
        for machine_suffix in machine_suffixes:
            self.verify_instance("{}-{}".format(self.getClusterName(),
                                                machine_suffix))


if __name__ == "__main__":
    unittest.main()
