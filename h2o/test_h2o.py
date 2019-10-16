"""This module provides testing functionality of the H2O Sparkling Water
Initialization Action.
"""

import unittest

from parameterized import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class H2OTestCase(DataprocTestCase):
    COMPONENT = "h2o"
    INIT_ACTIONS = ["h2o/h2o.sh"]
    SAMPLE_H2O_JOB_PATH = "h2o/sample-script.py"

    @parameterized.expand(
        [
            ("SINGLE", "1.3"),
            ("STANDARD", "1.3"),
            ("HA", "1.3"),
            ("SINGLE", "1.4"),
            ("STANDARD", "1.4"),
            ("HA", "1.4"),
        ],
        testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_h2o(self, configuration, dataproc_version):
        init_actions = self.INIT_ACTIONS
        optional_components = "ANACONDA"
        if dataproc_version == "1.3":
            init_actions = ["conda/bootstrap-conda.sh"] + init_actions
            optional_components = None

        self.createCluster(
            configuration,
            init_actions,
            dataproc_version,
            optional_components=optional_components,
            scopes="https://www.googleapis.com/auth/cloud-platform",
            machine_type="n1-standard-2")

        self.assert_dataproc_job(
            self.name, "pyspark", "{}/{}".format(self.INIT_ACTIONS_REPO,
                                                 self.SAMPLE_H2O_JOB_PATH))


if __name__ == "__main__":
    unittest.main()
