import unittest
import os

from parameterized import parameterized

from dataproc_test_case import DataprocTestCase

METADATA = 'flink-start-yarn-session=false'


class FlinkTestCase(DataprocTestCase):
    COMPONENT = 'flink'
    INIT_ACTION = 'gs://dataproc-initialization-actions/flink/flink.sh'
    TEST_SCRIPT_FILE_NAME = 'validate.sh'

    def verify_instance(self, name, yarn_session=True):
        self.upload_test_file(name)
        self.__run_test_file(name, yarn_session)
        self.remove_test_script(name)

    def __run_test_file(self, name, yarn_session):
        if yarn_session == True:
            cmd = '"bash {}"'.format(
                self.TEST_SCRIPT_FILE_NAME
            )
        else:
            cmd = '"bash {} {}"'.format(
                self.TEST_SCRIPT_FILE_NAME,
                yarn_session
            )

        ret_code, stdout, stderr = self.ssh_cmd(name, cmd)
        print("stdout", stdout)
        print("retcode", ret_code)
        print("stderr", stderr)
        self.assertEqual(ret_code, 0, "Failed to run test file. Error: {}".format(stderr))

    @parameterized.expand([
        ("SINGLE", "1.1", ["m"], METADATA),
        ("STANDARD", "1.1", ["m"], None),
        ("HA", "1.1", ["m-0", "m-1", "m-2"], None),
        ("SINGLE", "1.2", ["m"], METADATA),
        ("STANDARD", "1.2", ["m"], None),
        ("HA", "1.2", ["m-0", "m-1", "m-2"], None),
    ], testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_flink(self, configuration, dataproc_version, machine_suffixes, metadata):
        self.createCluster(configuration, self.INIT_ACTION, dataproc_version, metadata=metadata)
        for machine_suffix in machine_suffixes:
            self.verify_instance(
                "{}-{}".format(
                    self.getClusterName(),
                    machine_suffix
                )
            )

    @parameterized.expand([
        ("STANDARD", "1.1", ["m"], METADATA),
        ("HA", "1.1", ["m-0", "m-1", "m-2"], METADATA),
        ("STANDARD", "1.2", ["m"], METADATA),
        ("HA", "1.2", ["m-0", "m-1", "m-2"], METADATA),
    ], testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_flink_with_optional_metadata(self, configuration, dataproc_version, machine_suffixes, metadata):
        self.createCluster(configuration, self.INIT_ACTION, dataproc_version, metadata=metadata)
        for machine_suffix in machine_suffixes:
            self.verify_instance(
                "{}-{}".format(
                    self.getClusterName(),
                    machine_suffix
                ),
                yarn_session=False
            )


if __name__ == '__main__':
    unittest.main()
