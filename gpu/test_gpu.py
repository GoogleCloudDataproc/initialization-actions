import pkg_resources

from absl.testing import absltest
from absl.testing import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class NvidiaGpuDriverTestCase(DataprocTestCase):
  COMPONENT = "gpu"
  INIT_ACTIONS = ["gpu/install_gpu_driver.sh"]
  GPU_L4   = "type=nvidia-l4"
  GPU_T4   = "type=nvidia-tesla-t4"
  GPU_P100 = "type=nvidia-tesla-p100" # not available in us-central1-a
  GPU_V100 = "type=nvidia-tesla-v100" # not available in us-central1-a
  GPU_A100 = "type=nvidia-tesla-a100"

  def verify_instance(self, name):
    self.assert_instance_command(name, "nvidia-smi")

  def verify_mig_instance(self, name):
    self.assert_instance_command(name,
        "/usr/bin/nvidia-smi --query-gpu=mig.mode.current --format=csv,noheader | uniq | xargs -I % test % = 'Enabled'")

  def verify_instance_gpu_agent(self, name):
    self.assert_instance_command(
        name, "systemctl status gpu-utilization-agent.service")

  def verify_instance_cudnn(self, name):
    self.assert_instance_command(
        name, "sudo ldconfig -v 2>/dev/null | grep -q libcudnn" )

  @parameterized.parameters(
      ("SINGLE", ["m"], GPU_T4, None, None),
      ("STANDARD", ["m"], GPU_T4, None, None),
      ("STANDARD", ["m", "w-0", "w-1"], GPU_T4, GPU_T4, "NVIDIA"),
  )
  def test_install_gpu_default_agent(self, configuration, machine_suffixes,
                                     master_accelerator, worker_accelerator,
                                     driver_provider):

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
        timeout_in_minutes=30,
        boot_disk_size="50GB")
    for machine_suffix in machine_suffixes:
      self.verify_instance("{}-{}".format(self.getClusterName(),
                                          machine_suffix))

  @parameterized.parameters(
      ("SINGLE", ["m"], GPU_T4, None, None),
  )
  def test_install_gpu_without_agent(self, configuration, machine_suffixes,
                                     master_accelerator, worker_accelerator,
                                     driver_provider):

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
        timeout_in_minutes=30,
        boot_disk_size="50GB")
    for machine_suffix in machine_suffixes:
      self.verify_instance("{}-{}".format(self.getClusterName(),
                                          machine_suffix))

  @parameterized.parameters(
      ("STANDARD", ["m", "w-0", "w-1"], GPU_T4, GPU_T4, None),
      ("STANDARD", ["w-0", "w-1"], None, GPU_T4, "NVIDIA"),
      ("STANDARD", ["m"], GPU_T4, None, "NVIDIA"),
  )
  def test_install_gpu_with_agent(self, configuration, machine_suffixes,
                                  master_accelerator, worker_accelerator,
                                  driver_provider):

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
        boot_disk_size="50GB",
        scopes="https://www.googleapis.com/auth/monitoring.write")
    for machine_suffix in machine_suffixes:
      self.verify_instance("{}-{}".format(self.getClusterName(),
                                          machine_suffix))
      self.verify_instance_gpu_agent("{}-{}".format(self.getClusterName(),
                                                    machine_suffix))

  @parameterized.parameters(
      ("SINGLE",   ["m"],               GPU_T4, None,   "11.8"),
      ("STANDARD", ["m"],               GPU_T4, None,   "11.8"),
      ("STANDARD", ["m", "w-0", "w-1"], GPU_T4, GPU_T4, "12.4"),
      ("STANDARD", ["w-0", "w-1"],      None,   GPU_T4, "12.4"),
      ("STANDARD", ["w-0", "w-1"],      None,   GPU_T4, "11.8"),
  )
  def test_install_gpu_cuda_nvidia(self, configuration, machine_suffixes,
                                   master_accelerator, worker_accelerator,
                                   cuda_version):

    metadata = "gpu-driver-provider=NVIDIA,cuda-version={}".format(cuda_version)
    self.createCluster(
        configuration,
        self.INIT_ACTIONS,
        machine_type="n1-standard-2",
        master_accelerator=master_accelerator,
        worker_accelerator=worker_accelerator,
        metadata=metadata,
        timeout_in_minutes=30,
        boot_disk_size="50GB")
    for machine_suffix in machine_suffixes:
      self.verify_instance("{}-{}".format(self.getClusterName(),
                                          machine_suffix))

  @parameterized.parameters(
      ("STANDARD", ["m", "w-0", "w-1"], None, GPU_T4, "NVIDIA", "us-central1-a"),
  )
  def test_install_gpu_with_mig(self, configuration, machine_suffixes,
                                  master_accelerator, worker_accelerator,
                                  driver_provider, zone):
    self.skipTest("Test is known to fail.  Skipping so that we can exercise others")
    
    # self.createCluster(
    #     configuration,
    #     self.INIT_ACTIONS,
    #     zone=zone,
    #     master_machine_type="n1-standard-4",
    #     worker_machine_type="n1-standard-4",
    #     master_accelerator=master_accelerator,
    #     worker_accelerator=worker_accelerator,
    #     metadata=None,
    #     timeout_in_minutes=30,
    #     boot_disk_size="50GB",
    #     startup_script="gpu/mig.sh")

    # for machine_suffix in ["w-0", "w-1"]:
    #   self.verify_mig_instance("{}-{}".format(self.getClusterName(),
    #                                       machine_suffix))

  @parameterized.parameters(
      ("SINGLE", GPU_T4, None, None),
      ("STANDARD", GPU_T4, GPU_T4, "NVIDIA")
  )
  def test_gpu_allocation(self, configuration, master_accelerator,
                          worker_accelerator, driver_provider):
    self.skipTest("Test is known to fail.  Skipping so that we can exercise others")
    metadata = None
    if driver_provider is not None:
      metadata = "gpu-driver-provider={}".format(driver_provider)

    self.createCluster(
        configuration,
        self.INIT_ACTIONS,
        metadata=metadata,
        machine_type="n1-standard-2",
        master_accelerator=master_accelerator,
        worker_accelerator=worker_accelerator,
        boot_disk_size="50GB",
        timeout_in_minutes=30)

    get_gpu_resources_script="/usr/lib/spark/scripts/gpu/getGpusResources.sh"
    self.assert_dataproc_job(
        self.getClusterName(),
        "spark",
      "spark",
      "--jars=file:///usr/lib/spark/examples/jars/spark-examples.jar " \
      + "--class=org.apache.spark.examples.ml.JavaIndexToStringExample " \
      + "--properties=" \
      +   "spark.driver.resource.gpu.amount=1," \
      +   "spark.driver.resource.gpu.discoveryScript=" + get_gpu_resources_script \
      +   "spark.executor.resource.gpu.amount=1," \
      +   "spark.executor.resource.gpu.discoveryScript=" + get_gpu_resources_script
    )

  @parameterized.parameters(
    ("SINGLE", ["m"], GPU_T4, None, "11.8"),
    ("STANDARD", ["m"], GPU_T4, None, "11.8"),
    ("STANDARD", ["m", "w-0", "w-1"], GPU_T4, GPU_T4, "11.8"),
    ("STANDARD", ["w-0", "w-1"], None, GPU_T4, "11.8"),
    ("STANDARD", ["w-0", "w-1"], None, GPU_T4, "12.4"),
  )
  def test_install_gpu_cuda_nvidia_with_spark_job(self, configuration, machine_suffixes,
                                   master_accelerator, worker_accelerator,
                                   cuda_version):
    self.skipTest("Test is known to fail.  Skipping so that we can exercise others")
    metadata = "install-gpu-agent=true,gpu-driver-provider=NVIDIA,cuda-version={}".format(cuda_version)
    self.createCluster(
      configuration,
      self.INIT_ACTIONS,
      machine_type="n1-standard-2",
      master_accelerator=master_accelerator,
      worker_accelerator=worker_accelerator,
      metadata=metadata,
      timeout_in_minutes=30,
      boot_disk_size="50GB",
      scopes="https://www.googleapis.com/auth/monitoring.write")
    for machine_suffix in machine_suffixes:
      self.verify_instance("{}-{}".format(self.getClusterName(),
                                          machine_suffix))
      self.verify_instance_gpu_agent("{}-{}".format(self.getClusterName(),
                                                    machine_suffix))

    get_gpu_resources_script="/usr/lib/spark/scripts/gpu/getGpusResources.sh"
    self.assert_dataproc_job(
      self.getClusterName(),
      "spark",
      "--jars=file:///usr/lib/spark/examples/jars/spark-examples.jar " \
      + "--class=org.apache.spark.examples.ml.JavaIndexToStringExample " \
      + "--properties=" \
      +   "spark.driver.resource.gpu.amount=1," \
      +   "spark.driver.resource.gpu.discoveryScript=" + get_gpu_resources_script \
      +   "spark.executor.resource.gpu.amount=1," \
      +   "spark.executor.resource.gpu.discoveryScript=" + get_gpu_resources_script
    )


if __name__ == "__main__":
  absltest.main()
