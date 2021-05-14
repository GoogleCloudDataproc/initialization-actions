import os

from absl.testing import absltest
from absl.testing import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase

class LLAPTestCase(DataprocTestCase):
    COMPONENT = 'hive-llap'
    INIT_ACTIONS = ['hive-llap/llap.sh']
    TEST_SCRIPT_FILE_NAME = 'run_hive_commands.py'
    ##llap requires zookeeper
    OPTIONAL_COMPONENTS = ["ZOOKEEPER"]
    ##need initaction repo bucket and the number of llap ndoes to deploy
    METADATA_num_executors="num-llap-nodes=1"
    METADATA_exec_size="exec_size_mb=1000"


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
                            metadata="init-actions-repo=" + self.INIT_ACTIONS_REPO,
                            machine_type="e2-standard-4",
                            boot_disk_size="500GB")
        for machine_suffix in machine_suffixes:
            self.verify_instance("{}-{}".format(self.getClusterName(),
                                                machine_suffix))

    @parameterized.parameters(
    ("STANDARD", ["m"]),
    ("HA", ["m-0"]))

    def test_llap_num_exec(self, configuration, machine_suffixes):
        if self.getImageOs() == 'centos':
            self.skipTest("Not supported in CentOS-based images")

        self.createCluster(configuration, 
                            self.INIT_ACTIONS, 
                            optional_components=self.OPTIONAL_COMPONENTS,
                            metadata=self.METADATA_num_executors + ",init-actions-repo=" + self.INIT_ACTIONS_REPO,
                            machine_type="e2-standard-4",
                            boot_disk_size="500GB")
        for machine_suffix in machine_suffixes:
            self.verify_instance("{}-{}".format(self.getClusterName(),
                                                machine_suffix))

    @parameterized.parameters(
    ("STANDARD", ["m"]),
    ("HA", ["m-0"]))

    def test_llap_exec_size(self, configuration, machine_suffixes):
        if self.getImageOs() == 'centos':
            self.skipTest("Not supported in CentOS-based images")

        self.createCluster(configuration, 
                            self.INIT_ACTIONS, 
                            optional_components=self.OPTIONAL_COMPONENTS,
                            metadata=self.METADATA_exec_size + ",init-actions-repo=" + self.INIT_ACTIONS_REPO,
                            machine_type="e2-standard-4",
                            boot_disk_size="500GB")
        for machine_suffix in machine_suffixes:
            self.verify_instance("{}-{}".format(self.getClusterName(),
                                                machine_suffix))


if __name__ == '__main__':
    absltest.main()