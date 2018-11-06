import unittest

from parameterized import parameterized
import os
from integration_tests.dataproc_test_case import DataprocTestCase

class DrillTestCase(DataprocTestCase):
    COMPONENT = 'drill'
    TEST_SCRIPT_FILE_NAME = 'validate.sh'
    INIT_ACTION = 'gs://dataproc-initialization-actions/drill/drill.sh'
    INIT_ACTION_FOR_STANDARD = '\'gs://dataproc-initialization-actions/zookeeper/zookeeper.sh\',\'gs://dataproc-initialization-actions/drill/drill.sh\''

    def verify_instance(self, name, config, target_node):
        self.upload_test_file(
            os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                self.TEST_SCRIPT_FILE_NAME
            ),
            name)
        self.__run_bash_test_file(name, config, target_node)
        self.remove_test_script(self.TEST_SCRIPT_FILE_NAME, name)

    def __run_bash_test_file(self, name, config, target_node):
        cmd = 'gcloud compute ssh {} -- "sudo bash {} {} {}"'.format(
            name,
            self.TEST_SCRIPT_FILE_NAME,
            config,
            target_node
        )
        ret_code, stdout, stderr = self.run_command(cmd)
        self.assertEqual(ret_code, 0, "Failed to run test file. Error: {}".format(stderr))
    '''
    Passing 'Without' indicates using drill-embedded instead of distributed one.
    Required for clusters without Zookeeper.
    '''
    @parameterized.expand([
        ("SINGLE", "1.3-deb9", INIT_ACTION, [("m", "m", "Without")]),
        ("SINGLE", "1.2-deb9", INIT_ACTION, [("m", "m", "Without")]),
        ("STANDARD", "1.3-deb9", INIT_ACTION_FOR_STANDARD, [("m", "w-0", "ZOOKEEPER"), ("m", "m", "ZOOKEEPER")]),
        ("STANDARD", "1.2-deb9", INIT_ACTION_FOR_STANDARD, [("m", "w-0", "ZOOKEEPER"), ("m", "m", "ZOOKEEPER")]),
        ("HA", "1.3-deb9", INIT_ACTION, [("m-0", "w-0", "ZOOKEEPER"), ("w-0", "m-1", "ZOOKEEPER")]),
        ("HA", "1.2-deb9", INIT_ACTION, [("m-0", "w-0", "ZOOKEEPER"), ("w-0", "m-1", "ZOOKEEPER")]),
        ], testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_drill(self, configuration, dataproc_version, init_action, options):
        self.createCluster(
            configuration,
            init_action,
            dataproc_version,
        )
        for option in options:
            machine_suffix = option[0]
            target_machine_suffix = option[1]
            zookeeper_mode = option[2]
            self.verify_instance(
                "{}-{}".format(self.getClusterName(), machine_suffix),
                zookeeper_mode,
                "{}-{}".format(self.getClusterName(), target_machine_suffix),
            )


if __name__ == '__main__':
    unittest.main()
