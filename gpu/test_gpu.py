import pkg_resources
import time

from absl.testing import absltest
from absl.testing import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class NvidiaGpuDriverTestCase(DataprocTestCase):
  COMPONENT = "gpu"
  INIT_ACTIONS = ["gpu/install_gpu_driver.sh"]
  GPU_L4   = "type=nvidia-l4"
  GPU_T4   = "type=nvidia-tesla-t4"
  GPU_V100 = "type=nvidia-tesla-v100" # not available in us-central1-a
  GPU_A100 = "type=nvidia-tesla-a100"
  GPU_H100 = "type=nvidia-h100-80gb,count=8"

  def verify_instance(self, name):
    # Verify that nvidia-smi works
    time.sleep(3) # Many failed nvidia-smi attempts have been caused by impatience
    self.assert_instance_command(name, "nvidia-smi", 1)

  def verify_pyspark(self, name):
    # Verify that pyspark works
    self.assert_instance_command(name, "echo 'from pyspark.sql import SparkSession ; SparkSession.builder.getOrCreate()' | pyspark -c spark.executor.resource.gpu.amount=1 -c spark.task.resource.gpu.amount=0.01", 1)

  def verify_mig_instance(self, name):
    self.assert_instance_command(name,
        "/usr/bin/nvidia-smi --query-gpu=mig.mode.current --format=csv,noheader | uniq | xargs -I % test % = 'Enabled'")

  def verify_instance_gpu_agent(self, name):
    self.assert_instance_command(
        name, "systemctl status gpu-utilization-agent.service")

  def verify_instance_cudnn(self, name):
    self.assert_instance_command(
        name, "sudo ldconfig -v 2>/dev/null | grep -q libcudnn" )

  def verify_instance_nvcc(self, name, cuda_version):
    self.assert_instance_command(
        name, "/usr/local/cuda-{}/bin/nvcc --version | grep 'release {}'".format(cuda_version,cuda_version) )

  def verify_instance_spark(self):
    self.assert_dataproc_job(
      self.getClusterName(),
      "spark",
      "--jars=file:///usr/lib/spark/examples/jars/spark-examples.jar " \
      + "--class=org.apache.spark.examples.ml.JavaIndexToStringExample " \
      + "--properties="                           \
      +   "spark.executor.resource.gpu.amount=1," \
      +   "spark.executor.cores=6,"               \
      +   "spark.executor.memory=4G,"             \
      +   "spark.task.resource.gpu.amount=0.333," \
      +   "spark.task.cpus=2,"                    \
      +   "spark.yarn.unmanagedAM.enabled=false"
    )

  @parameterized.parameters(
      ("SINGLE",   ["m"], GPU_T4, None, None),
#      ("STANDARD", ["m"], GPU_T4, None, None),
      ("STANDARD", ["m", "w-0", "w-1"], GPU_T4, GPU_T4, "NVIDIA"),
  )
  def test_install_gpu_default_agent(self, configuration, machine_suffixes,
                                     master_accelerator, worker_accelerator,
                                     driver_provider):
    if ( self.getImageOs() == 'rocky' ) and self.getImageVersion() >= pkg_resources.parse_version("2.2"):
      self.skipTest("GPU drivers are currently FTBFS on Rocky 9 ; base dataproc image out of date")

    metadata = None
    if driver_provider is not None:
      metadata = "gpu-driver-provider={}".format(driver_provider)
    self.createCluster(
        configuration,
        self.INIT_ACTIONS,
        machine_type="n1-highmem-8",
        master_accelerator=master_accelerator,
        worker_accelerator=worker_accelerator,
        metadata=metadata,
        timeout_in_minutes=90,
        boot_disk_size="50GB")
    for machine_suffix in machine_suffixes:
      machine_name="{}-{}".format(self.getClusterName(),machine_suffix)
      self.verify_instance(machine_name)
      if ( self.getImageOs() != 'rocky' ) or ( configuration != 'SINGLE' ) or ( configuration == 'SINGLE' and self.getImageOs() == 'rocky' and self.getImageVersion() > pkg_resources.parse_version("2.1") ):
        self.verify_pyspark(machine_name)

  @parameterized.parameters(
      ("SINGLE", ["m"], GPU_T4, None, None),
  )
  def test_install_gpu_without_agent(self, configuration, machine_suffixes,
                                     master_accelerator, worker_accelerator,
                                     driver_provider):

    self.skipTest("No need to regularly test not installing the agent")

    if ( self.getImageOs() == 'rocky' ) and self.getImageVersion() >= pkg_resources.parse_version("2.2"):
      self.skipTest("GPU drivers are currently FTBFS on Rocky 9 ; base dataproc image out of date")

    metadata = "install-gpu-agent=false"
    if driver_provider is not None:
      metadata += ",gpu-driver-provider={}".format(driver_provider)
    self.createCluster(
        configuration,
        self.INIT_ACTIONS,
        machine_type="n1-highmem-8",
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
#      ("STANDARD", ["w-0", "w-1"], None, GPU_T4, "NVIDIA"),
#      ("STANDARD", ["m"], GPU_T4, None, "NVIDIA"),
  )
  def test_install_gpu_with_agent(self, configuration, machine_suffixes,
                                  master_accelerator, worker_accelerator,
                                  driver_provider):
    if ( self.getImageOs() == 'rocky' ) and self.getImageVersion() >= pkg_resources.parse_version("2.2"):
      self.skipTest("GPU drivers are currently FTBFS on Rocky 9 ; base dataproc image out of date")

    metadata = "install-gpu-agent=true"
    if driver_provider is not None:
      metadata += ",gpu-driver-provider={}".format(driver_provider)
    self.createCluster(
        configuration,
        self.INIT_ACTIONS,
        machine_type="n1-highmem-8",
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
#       ("SINGLE", ["m"],               GPU_T4, None,   "12.0"),
        ("SINGLE", ["m"],               GPU_T4, None,   "11.8"),
      ("STANDARD", ["m", "w-0", "w-1"], GPU_T4, GPU_T4, "12.4"),
#     ("STANDARD", ["w-0", "w-1"],      None,   GPU_T4, "11.8"),
  )
  def test_install_gpu_cuda_nvidia(self, configuration, machine_suffixes,
                                   master_accelerator, worker_accelerator,
                                   cuda_version):
    if ( self.getImageOs() == 'rocky' ) and self.getImageVersion() >= pkg_resources.parse_version("2.2"):
      self.skipTest("GPU drivers are currently FTBFS on Rocky 9 ; base dataproc image out of date")

    if pkg_resources.parse_version(cuda_version) == pkg_resources.parse_version("12.0") \
    and ( self.getImageOs() == 'debian' and self.getImageVersion() >= pkg_resources.parse_version("2.2") ):
      self.skipTest("CUDA == 12.0 not supported on debian 12")

    if pkg_resources.parse_version(cuda_version) > pkg_resources.parse_version("12.0") \
    and ( ( self.getImageOs() == 'ubuntu' and self.getImageVersion() <= pkg_resources.parse_version("2.0") ) or \
          ( self.getImageOs() == 'debian' and self.getImageVersion() <= pkg_resources.parse_version("2.1") ) ):
      self.skipTest("CUDA > 12.0 not supported on older debian/ubuntu releases")

    if pkg_resources.parse_version(cuda_version) < pkg_resources.parse_version("12.0") \
    and ( self.getImageOs() == 'debian' or self.getImageOs() == 'rocky' ) \
    and self.getImageVersion() >= pkg_resources.parse_version("2.2"):
      self.skipTest("CUDA < 12 not supported on Debian >= 12, Rocky >= 9")

    metadata = "gpu-driver-provider=NVIDIA,cuda-version={}".format(cuda_version)
    self.createCluster(
        configuration,
        self.INIT_ACTIONS,
        machine_type="n1-highmem-8",
        master_accelerator=master_accelerator,
        worker_accelerator=worker_accelerator,
        metadata=metadata,
        timeout_in_minutes=30,
        boot_disk_size="50GB")
    for machine_suffix in machine_suffixes:
      machine_name="{}-{}".format(self.getClusterName(),machine_suffix)
      self.verify_instance(machine_name)
      self.verify_instance_nvcc(machine_name, cuda_version)

  @parameterized.parameters(
      ("STANDARD", ["m"], GPU_H100, GPU_A100, "NVIDIA", "11.8"),
#      ("STANDARD", ["m"], GPU_H100, GPU_A100, "NVIDIA", "12.0"),
      ("STANDARD", ["m"], GPU_H100, GPU_A100, "NVIDIA", "12.4"),
  )
  def test_install_gpu_with_mig(self, configuration, machine_suffixes,
                                  master_accelerator, worker_accelerator,
                                  driver_provider, cuda_version):

    self.skipTest("Test is known to fail.  Skipping so that we can exercise others")

    if ( self.getImageOs() == 'rocky' ) and self.getImageVersion() >= pkg_resources.parse_version("2.2"):
      self.skipTest("GPU drivers are currently FTBFS on Rocky 9 ; base dataproc image out of date")

    if pkg_resources.parse_version(cuda_version) == pkg_resources.parse_version("12.0") \
    and ( self.getImageOs() == 'debian' and self.getImageVersion() >= pkg_resources.parse_version("2.2") ):
      self.skipTest("CUDA == 12.0 not supported on debian 12")

    if pkg_resources.parse_version(cuda_version) > pkg_resources.parse_version("12.0") \
    and ( ( self.getImageOs() == 'ubuntu' and self.getImageVersion() <= pkg_resources.parse_version("2.0") ) or \
          ( self.getImageOs() == 'debian' and self.getImageVersion() <= pkg_resources.parse_version("2.1") ) ):
      self.skipTest("CUDA > 12.0 not supported on older debian/ubuntu releases")

    if pkg_resources.parse_version(cuda_version) < pkg_resources.parse_version("12.0") \
    and ( self.getImageOs() == 'debian' or self.getImageOs() == 'rocky' ) \
    and self.getImageVersion() >= pkg_resources.parse_version("2.2"):
      self.skipTest("CUDA < 12 not supported on Debian >= 12, Rocky >= 9")

    metadata = "gpu-driver-provider={},cuda-version={}".format(driver_provider, cuda_version)

    self.createCluster(
        configuration,
        self.INIT_ACTIONS,
        master_machine_type="a3-highgpu-8g",
        worker_machine_type="a2-highgpu-2g",
        master_accelerator=master_accelerator,
        worker_accelerator=worker_accelerator,
        metadata=metadata,
        timeout_in_minutes=30,
        boot_disk_size="50GB",
        startup_script="gpu/mig.sh")

    for machine_suffix in ["w-0", "w-1"]:
      self.verify_mig_instance("{}-{}".format(self.getClusterName(),
                                          machine_suffix))

  @parameterized.parameters(
      ("SINGLE", GPU_T4, None, None),
      ("STANDARD", GPU_T4, GPU_T4, "NVIDIA")
  )
  def test_gpu_allocation(self, configuration, master_accelerator,
                          worker_accelerator, driver_provider):
    if ( self.getImageOs() == 'rocky' ) and self.getImageVersion() >= pkg_resources.parse_version("2.2"):
      self.skipTest("GPU drivers are currently FTBFS on Rocky 9 ; base dataproc image out of date")

    if ( self.getImageOs() == 'rocky' ) and self.getImageVersion() <= pkg_resources.parse_version("2.1") \
    and configuration == 'SINGLE':
      self.skipTest("2.1-rocky8 and 2.0-rocky8 single instance tests are known to fail with errors about nodes_include being empty")

    metadata = None
    if driver_provider is not None:
      metadata = "gpu-driver-provider={}".format(driver_provider)

    self.createCluster(
        configuration,
        self.INIT_ACTIONS,
        metadata=metadata,
        machine_type="n1-highmem-8",
        master_accelerator=master_accelerator,
        worker_accelerator=worker_accelerator,
        boot_disk_size="50GB",
        timeout_in_minutes=30)

    self.verify_instance_spark()

  @parameterized.parameters(
    ("SINGLE", ["m"], GPU_T4, None, "11.8"),
#    ("STANDARD", ["m"], GPU_T4, None, "12.0"),
    ("STANDARD", ["m", "w-0", "w-1"], GPU_T4, GPU_T4, "12.4"),
#    ("STANDARD", ["w-0", "w-1"], None, GPU_T4, "11.8"),
#    ("STANDARD", ["w-0", "w-1"], None, GPU_T4, "12.0"),
  )
  def test_install_gpu_cuda_nvidia_with_spark_job(self, configuration, machine_suffixes,
                                   master_accelerator, worker_accelerator,
                                   cuda_version):

    if ( self.getImageOs() == 'rocky' ) and self.getImageVersion() >= pkg_resources.parse_version("2.2"):
      self.skipTest("GPU drivers are currently FTBFS on Rocky 9 ; base dataproc image out of date")

    if ( self.getImageOs() == 'rocky' ) and self.getImageVersion() <= pkg_resources.parse_version("2.1") \
    and configuration == 'SINGLE':
      self.skipTest("2.1-rocky8 and 2.0-rocky8 single instance tests fail with errors about nodes_include being empty")

    if pkg_resources.parse_version(cuda_version) == pkg_resources.parse_version("12.0") \
    and ( self.getImageOs() == 'debian' and self.getImageVersion() >= pkg_resources.parse_version("2.2") ):
      self.skipTest("CUDA == 12.0 not supported on debian 12")

    if pkg_resources.parse_version(cuda_version) > pkg_resources.parse_version("12.0") \
    and ( ( self.getImageOs() == 'ubuntu' and self.getImageVersion() <= pkg_resources.parse_version("2.0") ) or \
          ( self.getImageOs() == 'debian' and self.getImageVersion() <= pkg_resources.parse_version("2.1") ) ):
      self.skipTest("CUDA > 12.0 not supported on older debian/ubuntu releases")

    if pkg_resources.parse_version(cuda_version) < pkg_resources.parse_version("12.0") \
    and ( self.getImageOs() == 'debian' or self.getImageOs() == 'rocky' ) \
    and self.getImageVersion() >= pkg_resources.parse_version("2.2"):
      self.skipTest("CUDA < 12 not supported on Debian >= 12, Rocky >= 9")

    metadata = "install-gpu-agent=true,gpu-driver-provider=NVIDIA,cuda-version={}".format(cuda_version)
    self.createCluster(
      configuration,
      self.INIT_ACTIONS,
      machine_type="n1-highmem-8",
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

    self.verify_instance_spark()

if __name__ == "__main__":
  absltest.main()
