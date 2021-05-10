import os

from absl.testing import absltest
from absl.testing import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class LLAPTestCase(DataprocTestCase):
    COMPONENT = 'llap'
    INIT_ACTIONS = ['llap/llap.sh']
    TEST_SCRIPT_FILE_NAME = 'run_hive_commands.py'

    def verify_instance(self, name):
        self.upload_test_file(
            os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                self.TEST_SCRIPT_FILE_NAME), name)
        self.__run_test_script(name)
        self.remove_test_script(self.TEST_SCRIPT_FILE_NAME, name)

    def __run_test_script(self, name):
        self.assert_instance_command(
            name, "python {}".format(self.TEST_SCRIPT_FILE_NAME))

    @parameterized.parameters(
        ("HA", ["m-0", "m-1", "m-2"]), )
    def test_llap(self, configuration, machine_suffixes):
        if self.getImageOs() == 'centos':
            self.skipTest("Not supported in CentOS-based images")

        self.createCluster(configuration, 
                            self.INIT_ACTIONS, 
                            metadata=None,
                            scopes=None,
                            properties=None,
                            timeout_in_minutes=None,
                            beta=False,
                            master_accelerator=None,
                            worker_accelerator=None,
                            optional_components=None,
                            machine_type="e2-standard-16",
                            boot_disk_size="500GB")
        for machine_suffix in machine_suffixes:
            self.verify_instance("{}-{}".format(self.getClusterName(),
                                                machine_suffix))


if __name__ == '__main__':
    absltest.main()