import unittest

from parameterized import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class TonYTestCase(DataprocTestCase):
    COMPONENT = 'tony'
    INIT_ACTIONS = ['tony/tony.sh']
    TONY_VERSION = '0.3.22'

    @parameterized.expand(
        [
            ("STANDARD", "1.2"),
            ("STANDARD", "1.3"),
        ],
        testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_tony(self, configuration, dataproc_version):
        self.createCluster(configuration,
                           self.INIT_ACTIONS,
                           dataproc_version,
                           timeout_in_minutes=30,
                           machine_type="n1-standard-2")

        # Verify cluster using TensorFlow job
        self.assert_dataproc_job(
            self.name, 'hadoop', '''\
                --class com.linkedin.tony.cli.ClusterSubmitter \
                --jars "file:///opt/tony/TonY-samples/tony-cli-{}-all.jar" \
                -- \
                --src_dir=/opt/tony/TonY-samples/jobs/TFJob/src \
                --task_params="--data_dir /tmp/ --working_dir /tmp/" \
                --conf_file=/opt/tony/TonY-samples/jobs/TFJob/tony.xml \
                --executes mnist_distributed.py \
                --python_venv=/opt/tony/TonY-samples/deps/tf.zip \
                --python_binary_path=tf/bin/python3
            '''.format(self.TONY_VERSION))

        # Verify cluster using PyTorch job
        self.assert_dataproc_job(
            self.name, 'hadoop', '''\
                --class com.linkedin.tony.cli.ClusterSubmitter \
                --jars "file:///opt/tony/TonY-samples/tony-cli-{}-all.jar" \
                -- \
                --src_dir=/opt/tony/TonY-samples/jobs/PTJob/src \
                --task_params="--root /tmp/" \
                --conf_file=/opt/tony/TonY-samples/jobs/PTJob/tony.xml \
                --executes mnist_distributed.py \
                --python_venv=/opt/tony/TonY-samples/deps/pytorch.zip \
                --python_binary_path=pytorch/bin/python3
            '''.format(self.TONY_VERSION))


if __name__ == '__main__':
    unittest.main()
