from absl.testing import absltest
from absl.testing import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class NvidiaGpuDriverTestCase(DataprocTestCase):
    COMPONENT = 'gpu'
    INIT_ACTIONS = ['gpu/install_gpu_driver.sh']
    GPU_V100 = 'type=nvidia-tesla-v100'

    def verify_instance(self, name):
        self.assert_instance_command(name, "nvidia-smi")

    def verify_instance_gpu_agent(self, name):
        self.assert_instance_command(
            name, "systemctl status gpu-utilization-agent.service")

    @parameterized.parameters(
        ("STANDARD", ["m"], GPU_V100, None, None),
        ("STANDARD", ["m", "w-0", "w-1"], GPU_V100, GPU_V100, "OS"),
        ("STANDARD", ["w-0", "w-1"], None, GPU_V100, "NVIDIA"),
    )
    def test_install_gpu_default_agent(self, configuration, machine_suffixes,
                                       master_accelerator, worker_accelerator,
                                       driver_provider):
        metadata = None
        if driver_provider is not None:
            metadata = "gpu-driver-provider={}".format(driver_provider)
        self.createCluster(configuration,
                           self.INIT_ACTIONS,
                           machine_type='n1-standard-2',
                           master_accelerator=master_accelerator,
                           worker_accelerator=worker_accelerator,
                           metadata=metadata,
                           timeout_in_minutes=30)
        for machine_suffix in machine_suffixes:
            self.verify_instance("{}-{}".format(self.getClusterName(),
                                                machine_suffix))

    @parameterized.parameters(
        ("STANDARD", ["w-0", "w-1"], None, GPU_V100, None),
        ("STANDARD", ["m"], GPU_V100, None, "OS"),
        ("STANDARD", ["m", "w-0", "w-1"], GPU_V100, GPU_V100, "NVIDIA"),
    )
    def test_install_gpu_without_agent(self, configuration, machine_suffixes,
                                       master_accelerator, worker_accelerator,
                                       driver_provider):
        metadata = 'install-gpu-agent=false'
        if driver_provider is not None:
            metadata += ",gpu-driver-provider={}".format(driver_provider)
        self.createCluster(configuration,
                           self.INIT_ACTIONS,
                           machine_type='n1-standard-2',
                           master_accelerator=master_accelerator,
                           worker_accelerator=worker_accelerator,
                           metadata=metadata,
                           timeout_in_minutes=30)
        for machine_suffix in machine_suffixes:
            self.verify_instance("{}-{}".format(self.getClusterName(),
                                                machine_suffix))

    @parameterized.parameters(
        ("STANDARD", ["m", "w-0", "w-1"], GPU_V100, GPU_V100, None),
        ("STANDARD", ["w-0", "w-1"], None, GPU_V100, "OS"),
        ("STANDARD", ["m"], GPU_V100, None, "NVIDIA"),
    )
    def test_install_gpu_with_agent(self, configuration, machine_suffixes,
                                    master_accelerator, worker_accelerator,
                                    driver_provider):
        metadata = 'install-gpu-agent=true'
        if driver_provider is not None:
            metadata += ",gpu-driver-provider={}".format(driver_provider)
        self.createCluster(
            configuration,
            self.INIT_ACTIONS,
            machine_type='n1-standard-2',
            master_accelerator=master_accelerator,
            worker_accelerator=worker_accelerator,
            metadata=metadata,
            timeout_in_minutes=30,
            scopes='https://www.googleapis.com/auth/monitoring.write')
        for machine_suffix in machine_suffixes:
            self.verify_instance("{}-{}".format(self.getClusterName(),
                                                machine_suffix))
            self.verify_instance_gpu_agent("{}-{}".format(
                self.getClusterName(), machine_suffix))

    @parameterized.parameters(
        ("STANDARD", ["m"], GPU_V100, None, "NVIDIA"),
        ("STANDARD", ["m", "w-0", "w-1"], GPU_V100, GPU_V100, "NVIDIA"),
        ("STANDARD", ["w-0", "w-1"], None, GPU_V100, "NVIDIA"),
    )
    def test_install_gpu_cuda_10_1(self, configuration, machine_suffixes,
                                       master_accelerator, worker_accelerator,
                                       driver_provider):
        metadata = "gpu-driver-provider={},cuda-version=10.1".format(driver_provider)
        self.createCluster(configuration,
                           self.INIT_ACTIONS,
                           machine_type='n1-standard-2',
                           master_accelerator=master_accelerator,
                           worker_accelerator=worker_accelerator,
                           metadata=metadata,
                           timeout_in_minutes=30)
        for machine_suffix in machine_suffixes:
            self.verify_instance("{}-{}".format(self.getClusterName(),
                                                machine_suffix))
if __name__ == '__main__':
    absltest.main()
