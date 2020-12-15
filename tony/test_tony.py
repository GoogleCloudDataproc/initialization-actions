import pkg_resources
from absl.testing import absltest
from absl.testing import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class TonYTestCase(DataprocTestCase):
    COMPONENT = 'tony'
    INIT_ACTIONS = ['tony/tony.sh']
    GPU_V100 = "type=nvidia-tesla-v100"
    GPU_INIT_ACTIONS = ['gpu/install_gpu_driver.sh'] + INIT_ACTIONS

    @parameterized.parameters(
        "SINGLE",
        "STANDARD",
    )
    def test_tony_tf(self, configuration):
        # Init action supported on Dataproc 1.5+
        if self.getImageVersion() < pkg_resources.parse_version("1.5"):
            self.skipTest("Not supported in pre 1.5 images")

        self.createCluster(
            configuration,
            self.INIT_ACTIONS,
            timeout_in_minutes=30,
            machine_type="e2-standard-4")

        # Verify a cluster using TensorFlow job
        self.assert_dataproc_job(
            self.name, 'hadoop', '''\
                --class=com.linkedin.tony.cli.ClusterSubmitter \
                --jars=file:///opt/tony/TonY-samples/tony-cli.jar \
                -- \
                --src_dir=/opt/tony/TonY-samples/jobs/TFJob/src \
                --task_params="--data_dir /tmp --working_dir /tmp" \
                --conf_file=/opt/tony/TonY-samples/jobs/TFJob/tony.xml \
                --executes=mnist_keras_distributed.py \
                --python_venv=/opt/tony/TonY-samples/deps/tf.zip \
                --python_binary_path=tf/bin/python3
            ''')


    @parameterized.parameters(
        "SINGLE",
        "STANDARD",
    )
    def test_tony_tf_gpu(self, configuration):
        # Init action supported on Dataproc 1.5+
        if self.getImageVersion() < pkg_resources.parse_version("2.0"):
            self.skipTest("TonY with GPUs not supported in pre 2.0 images")

        metadata ="tf_gpu=true,cuda-version=11.0,cudnn-version=8.0.5.39"
        self.createCluster(
            configuration,
            self.GPU_INIT_ACTIONS,
            timeout_in_minutes=30,
            metadata=metadata,
            master_accelerator=self.GPU_V100,
            worker_accelerator=self.GPU_V100,
            machine_type="n1-standard-4")

        # Verify a cluster using TensorFlow job
        self.assert_dataproc_job(
            self.name, 'hadoop', '''\
                --class=com.linkedin.tony.cli.ClusterSubmitter \
                --jars=file:///opt/tony/TonY-samples/tony-cli.jar \
                -- \
                --src_dir=/opt/tony/TonY-samples/jobs/TFJob/src \
                --task_params="--data_dir /tmp --working_dir /tmp" \
                --conf_file=/opt/tony/TonY-samples/jobs/TFJob/tony.xml \
                --executes=mnist_keras_distributed.py \
                --python_venv=/opt/tony/TonY-samples/deps/tf.zip \
                --python_binary_path=tf/bin/python3
            ''')


if __name__ == '__main__':
    absltest.main()
