import sys
import unittest

from absl import flags
from parameterized import parameterized
from integration_tests.dataproc_test_case import DataprocTestCase

FLAGS = flags.FLAGS
flags.DEFINE_multi_string('params', '', 'Configuration to test')
FLAGS(sys.argv)


class NvidiaGpuDriverTestCase(DataprocTestCase):
    COMPONENT = 'gpu'
    INIT_ACTIONS = ['gpu/install_gpu_driver.sh']
    MASTER_GPU_TYPE = 'type=nvidia-tesla-v100'
    WORKER_GPU_TYPE = 'type=nvidia-tesla-v100'

    def verify_instance(self, name):
        self.assert_instance_command(name, "nvidia-smi")

    def verify_instance_gpu_agent(self, name):
        self.assert_instance_command(
            name, "systemctl status gpu_utilization_agent.service")

    def buildParameters():
        """Builds parameters from flags arguments passed to the test."""
        flags_parameters = FLAGS.params
        params = []
        if not flags_parameters[0]:
            # Default parameters
            params = [
                ("STANDARD", "1.2", ["m", "w-0"
                                 ], MASTER_GPU_TYPE, WORKER_GPU_TYPE),
                ("STANDARD", "1.3", ["m", "w-0"
                                     ], MASTER_GPU_TYPE, WORKER_GPU_TYPE),
                ("STANDARD", "1.4", ["m", "w-0"
                                     ], MASTER_GPU_TYPE, WORKER_GPU_TYPE),
            ]
        else:
            for param in flags_parameters:
                (config, version, machine_suffixes, master_gpu_type, worker_gpu_type) = param.split()
                machine_suffixes = (machine_suffixes.split(',')
                    if ',' in machine_suffixes
                    else [machine_suffixes])
                params.append((config, version, machine_suffixes, master_gpu_type, worker_gpu_type))
        return params

    @parameterized.expand(
        buildParameters(),
        testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_install_gpu(self, configuration, dataproc_version,
                         machine_suffixes, master_accelerator,
                         worker_accelerator):
        init_actions = self.INIT_ACTIONS
        self.createCluster(configuration,
                           init_actions,
                           dataproc_version,
                           beta=True,
                           master_accelerator=master_accelerator,
                           worker_accelerator=worker_accelerator)
        for machine_suffix in machine_suffixes:
            self.verify_instance("{}-{}".format(self.getClusterName(),
                                                machine_suffix))

    @parameterized.expand(
        buildParameters(),
        testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_install_gpu_no_agent(self, configuration, dataproc_version,
                                  machine_suffixes, master_accelerator,
                                  worker_accelerator):
        init_actions = self.INIT_ACTIONS
        self.createCluster(configuration,
                           init_actions,
                           dataproc_version,
                           beta=True,
                           master_accelerator=master_accelerator,
                           worker_accelerator=worker_accelerator,
                           metadata='install_gpu_agent=false')
        for machine_suffix in machine_suffixes:
            self.verify_instance("{}-{}".format(self.getClusterName(),
                                                machine_suffix))

    @parameterized.expand(
        buildParameters(),
        testcase_func_name=DataprocTestCase.generate_verbose_test_name)
    def test_install_gpu_agent(self, configuration, dataproc_version,
                               machine_suffixes, master_accelerator,
                               worker_accelerator):

        init_actions = self.INIT_ACTIONS
        self.createCluster(
            configuration,
            init_actions,
            dataproc_version,
            beta=True,
            master_accelerator=master_accelerator,
            worker_accelerator=worker_accelerator,
            metadata='install_gpu_agent=true',
            scopes='https://www.googleapis.com/auth/monitoring.write')
        for machine_suffix in machine_suffixes:
            self.verify_instance("{}-{}".format(self.getClusterName(),
                                                machine_suffix))
            self.verify_instance_gpu_agent("{}-{}".format(
                self.getClusterName(), machine_suffix))


if __name__ == '__main__':
    del sys.argv[1:]
    unittest.main()
