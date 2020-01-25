from absl.testing import absltest
from absl.testing import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class SparkGPUTestCase(DataprocTestCase):
    COMPONENT = 'spark-gpu'
    INIT_ACTIONS = ['spark-gpu/rapids.sh']

    def verify_instance(self, name):
        self.assert_instance_command(name, "nvidia-smi")

    @parameterized.parameters(
        ("STANDARD", ["w-0"], 'type=nvidia-tesla-t4,count=1'), )
    def test_spark_gpu(self, configuration, machine_suffixes, worker_accelerator):
        init_actions = self.INIT_ACTIONS

        self.createCluster(
            configuration,
            init_actions,
            beta=True,
            worker_accelerator=worker_accelerator)
        for machine_suffix in machine_suffixes:
            self.verify_instance("{}-{}".format(self.getClusterName(),
                                                machine_suffix))


if __name__ == '__main__':
    absltest.main()
