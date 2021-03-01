import pkg_resources
from absl.testing import absltest
from absl.testing import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class NvidiaGpuDriverTestCase(DataprocTestCase):
  COMPONENT = "gpu"
  INIT_ACTIONS = ["gpu/install_gpu_driver.sh"]
  GPU_V100 = "type=nvidia-tesla-v100"

  def verify_instance(self, name):
    self.assert_instance_command(name, "nvidia-smi")

  def verify_instance_gpu_agent(self, name):
    self.assert_instance_command(
        name, "systemctl status gpu-utilization-agent.service")

  def verify_instance_cudnn(self, name):
    self.assert_instance_command(
        name, "sudo ldconfig -p | grep -q libcudnn" )

  @parameterized.parameters(
      ("SINGLE", ["m"], GPU_V100, None, None),
      ("STANDARD", ["m"], GPU_V100, None, None),
      ("STANDARD", ["m", "w-0", "w-1"], GPU_V100, GPU_V100, "OS"),
      ("STANDARD", ["w-0", "w-1"], None, GPU_V100, "NVIDIA"),
  )
  def test_install_gpu_default_agent(self, configuration, machine_suffixes,
                                     master_accelerator, worker_accelerator,
                                     driver_provider):
    if self.getImageOs() == 'centos':
      self.skipTest("Not supported in CentOS-based images")

    metadata = None
    if driver_provider is not None:
      metadata = "gpu-driver-provider={}".format(driver_provider)
    self.createCluster(
        configuration,
        self.INIT_ACTIONS,
        machine_type="n1-standard-2",
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
    if self.getImageOs() == 'centos':
      self.skipTest("Not supported in CentOS-based images")

    metadata = "install-gpu-agent=false"
    if driver_provider is not None:
      metadata += ",gpu-driver-provider={}".format(driver_provider)
    self.createCluster(
        configuration,
        self.INIT_ACTIONS,
        machine_type="n1-standard-2",
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
    if self.getImageOs() == 'centos':
      self.skipTest("Not supported in CentOS-based images")

    # GPU agent supported on Dataproc 1.4+
    if self.getImageVersion() < pkg_resources.parse_version("1.4"):
      self.skipTest("GPU utiliziation metrics only supported on Dataproc 1.4+")
    
    metadata = "install-gpu-agent=true"
    if driver_provider is not None:
      metadata += ",gpu-driver-provider={}".format(driver_provider)
    self.createCluster(
        configuration,
        self.INIT_ACTIONS,
        machine_type="n1-standard-2",
        master_accelerator=master_accelerator,
        worker_accelerator=worker_accelerator,
        metadata=metadata,
        timeout_in_minutes=30,
        scopes="https://www.googleapis.com/auth/monitoring.write")
    for machine_suffix in machine_suffixes:
      self.verify_instance("{}-{}".format(self.getClusterName(),
                                          machine_suffix))
      self.verify_instance_gpu_agent("{}-{}".format(self.getClusterName(),
                                                    machine_suffix))

  @parameterized.parameters(
      ("SINGLE", ["m"], GPU_V100, None, "10.1"),
      ("STANDARD", ["m"], GPU_V100, None, "10.2"),
      ("STANDARD", ["m", "w-0", "w-1"], GPU_V100, GPU_V100, "11.0"),
      ("STANDARD", ["w-0", "w-1"], None, GPU_V100, "11.1"),
  )
  def test_install_gpu_cuda_nvidia(self, configuration, machine_suffixes,
                                   master_accelerator, worker_accelerator,
                                   cuda_version):
    if self.getImageOs() == 'centos':
      self.skipTest("Not supported in CentOS-based images")

    metadata = "gpu-driver-provider=NVIDIA,cuda-version={}".format(cuda_version)
    self.createCluster(
        configuration,
        self.INIT_ACTIONS,
        machine_type="n1-standard-2",
        master_accelerator=master_accelerator,
        worker_accelerator=worker_accelerator,
        metadata=metadata,
        timeout_in_minutes=30)
    for machine_suffix in machine_suffixes:
      self.verify_instance("{}-{}".format(self.getClusterName(),
                                          machine_suffix))

  @parameterized.parameters(
      ("STANDARD", ["m", "w-0", "w-1"], GPU_V100, GPU_V100, "NVIDIA", "8.0.5.39"),
  )
  def test_install_gpu_with_cudnn(self, configuration, machine_suffixes,
                                  master_accelerator, worker_accelerator,
                                  driver_provider, cudnn_version):
    if self.getImageOs() == 'centos':
      self.skipTest("Not supported in CentOS-based images")

    metadata = "cudnn-version={}".format(cudnn_version)
    if driver_provider is not None:
      metadata += ",gpu-driver-provider={}".format(driver_provider)
    self.createCluster(
        configuration,
        self.INIT_ACTIONS,
        machine_type="n1-standard-2",
        master_accelerator=master_accelerator,
        worker_accelerator=worker_accelerator,
        metadata=metadata,
        timeout_in_minutes=30,
        scopes="https://www.googleapis.com/auth/monitoring.write")
    for machine_suffix in machine_suffixes:
      self.verify_instance_cudnn("{}-{}".format(self.getClusterName(),
                                                machine_suffix))

if __name__ == "__main__":
  absltest.main()
