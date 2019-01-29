import unittest
import os
from parameterized import parameterized
from integration_tests.dataproc_test_case import DataprocTestCase
import datetime


class DruidTestCase(DataprocTestCase):
    COMPONENT = 'druid'
    INIT_ACTION = '\'gs://dataproc-initialization-actions/druid/druid.sh\''
    INIT_ACTION_FOR_STANDARD = '\'gs://dataproc-initialization-actions/zookeeper/zookeeper.sh\',\'gs://dataproc-initialization-actions/druid/druid.sh\''
    HELPER_ACTIONS = 'gs://dataproc-initialization-actions/cloud-sql-proxy/cloud-sql-proxy.sh'
    TEST_SCRIPT_FILE_NAME = 'verify_druid_running.py'
    SCOPES = 'sql-admin'
    DB_NAME = None

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        _, region, _ = cls.run_command("gcloud config get-value compute/region")
        cls.REGION = region.strip() or "global"
        _, project, _ = cls.run_command("gcloud config get-value project")
        project = project.strip()
        cls.PROJECT_METADATA = '{}:{}'.format(project, cls.REGION)

    def verify_instance(self, name):
        self.upload_test_file(
            os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                self.TEST_SCRIPT_FILE_NAME
            ),
            name)
        self.__run_command_on_cluster(name, 'yes | sudo apt-get install python3-pip')
        self.__run_command_on_cluster(name, 'sudo pip3 install requests wget')
        self.__run_test_file(name)
        self.remove_test_script(self.TEST_SCRIPT_FILE_NAME, name)

    def __run_test_file(self, name):
        cmd = 'gcloud compute ssh {} -- "sudo python3 {} "'.format(
            name,
            self.TEST_SCRIPT_FILE_NAME
        )
        ret_code, stdout, stderr = self.run_command(cmd)
        self.assertEqual(ret_code, 0, "Failed to run test file. Error: {}".format(stderr))

    def __run_command_on_cluster(self, name, command):
        cmd = 'gcloud compute ssh {} -- "{} "'.format(
            name,
            command
        )
        ret_code, stdout, stderr = self.run_command(cmd)
        self.assertEqual(ret_code, 0, "Failed to run command. Error: {}".format(stderr))

    @parameterized.expand([
        ("SINGLE", "1.0", INIT_ACTION, ["m"]),
        ("STANDARD", "1.0", INIT_ACTION_FOR_STANDARD, ["m"]),
        ("HA", "1.0", INIT_ACTION, ["m-0", "m-1", "m-2"]),
        ("SINGLE", "1.1", INIT_ACTION, ["m"]),
        ("STANDARD", "1.1", INIT_ACTION_FOR_STANDARD, ["m"]),
        ("HA", "1.1", INIT_ACTION, ["m-0", "m-1", "m-2"]),
        ("SINGLE", "1.2", INIT_ACTION, ["m"]),
        ("STANDARD", "1.2", INIT_ACTION_FOR_STANDARD, ["m"]),
        ("HA", "1.2", INIT_ACTION, ["m-0", "m-1", "m-2"]),
        ("SINGLE", "1.3", INIT_ACTION, ["m"]),
        ("STANDARD", "1.3", INIT_ACTION_FOR_STANDARD, ["m"]),
        ("HA", "1.3", INIT_ACTION, ["m-0", "m-1"]),

    ], testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_druid(self, configuration, dataproc_version, init_action, machine_suffixes):
        self.createCluster(
            configuration,
            init_action,
            dataproc_version,
            timeout_in_minutes=25
        )
        for machine_suffix in machine_suffixes:
            self.verify_instance(
                "{}-{}".format(
                    self.getClusterName(),
                    machine_suffix
                )

            )

    @parameterized.expand([
        ("SINGLE", "1.0", INIT_ACTION, ["m"], HELPER_ACTIONS),
        ("STANDARD", "1.0", INIT_ACTION_FOR_STANDARD, ["m"], HELPER_ACTIONS),
        ("HA", "1.0", INIT_ACTION, ["m-0", "m-1", "m-2"], HELPER_ACTIONS),
        ("SINGLE", "1.1", INIT_ACTION, ["m"], HELPER_ACTIONS),
        ("STANDARD", "1.1", INIT_ACTION_FOR_STANDARD, ["m"], HELPER_ACTIONS),
        ("HA", "1.1", INIT_ACTION, ["m-0", "m-1", "m-2"], HELPER_ACTIONS),
        ("SINGLE", "1.2", INIT_ACTION, ["m"], HELPER_ACTIONS),
        ("STANDARD", "1.2", INIT_ACTION_FOR_STANDARD, ["m"], HELPER_ACTIONS),
        ("HA", "1.2", INIT_ACTION, ["m-0", "m-1", "m-2"], HELPER_ACTIONS),
        ("SINGLE", "1.3", INIT_ACTION, ["m"], HELPER_ACTIONS),
        ("STANDARD", "1.3", INIT_ACTION_FOR_STANDARD, ["m"], HELPER_ACTIONS),
        ("HA", "1.3", INIT_ACTION, ["m-0", "m-1"], HELPER_ACTIONS),

    ], testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_druid_with_cloud_sql(self, configuration, dataproc_version, init_action, machine_suffixes, helper_actions):
        if helper_actions:
            init_actions = "{},{}".format(helper_actions, init_action)
            self.DB_NAME = "test-{}-db".format(datetime.datetime.now().strftime("%Y%m%d%H%M%S"))
            print('gcloud sql instances create {} --region {}'.format(self.DB_NAME, self.REGION))
            ret_code, stdout, stderr = self.run_command(
                'gcloud sql instances create {} --region {}'.format(self.DB_NAME, self.REGION)
            )
        else:
            init_actions = init_action

        self.createCluster(
            configuration,
            init_actions,
            dataproc_version,
            metadata='hive-metastore-instance={}:{}'.format(
                self.PROJECT_METADATA, self.DB_NAME
            ),
            scopes=self.SCOPES,
            timeout_in_minutes=25
        )
        for machine_suffix in machine_suffixes:
            self.verify_instance(
                "{}-{}".format(
                    self.getClusterName(),
                    machine_suffix
                )

            )
        if helper_actions:
            ret_code, stdout, stderr = self.run_command(
                'gcloud sql instances delete {}'.format(self.DB_NAME)
            )


if __name__ == '__main__':
    unittest.main()
