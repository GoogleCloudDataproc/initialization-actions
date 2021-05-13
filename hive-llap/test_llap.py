import os

from absl.testing import absltest
from absl.testing import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase

class LLAPTestCase(DataprocTestCase):
    COMPONENT = 'hive-llap'
    INIT_ACTIONS = ['hive-llap/llap.sh']
    TEST_SCRIPT_FILE_NAME = 'run_hive_commands.py'
    OPTIONAL_COMPONENTS = ["ZOOKEEPER"]
    ##need initaction repo bucket and the number of llap ndoes to deploy
    METADATA="num-llap-nodes=1,init-actions-repo=gs://[]"


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
        ("HA", ["m-0"]),
        ("STANDARD", ["m"]))
    def test_llap(self, configuration, machine_suffixes):
        if self.getImageOs() == 'centos':
            self.skipTest("Not supported in CentOS-based images")

        self.createCluster(configuration, 
                            self.INIT_ACTIONS, 
                            optional_components=self.OPTIONAL_COMPONENTS,
                            metadata=self.METADATA,
                            machine_type="e2-standard-8",
                            boot_disk_size="500GB")
        for machine_suffix in machine_suffixes:
            self.verify_instance("{}-{}".format(self.getClusterName(),
                                                machine_suffix))


if __name__ == '__main__':
    absltest.main()