import pkg_resources
import time
import random
from absl.testing import absltest
from absl.testing import parameterized
from gpu.gpu_test_case_base import GpuTestCaseBase

DEFAULT_TIMEOUT = 45  # minutes
DEFAULT_CUDA_VERSION = "12.4"

class AutomatedGpuTestCase(GpuTestCaseBase):
  """
  Extends GpuTestCaseBase with behaviors specific to automated test runs,
  such as adding delays to avoid race conditions.
  """
  def verify_instance(self, name):
    # Add delay to stagger checks in automated environments
    sleep_duration = 3 + random.randint(1, 30)
    print(f"Waiting for {sleep_duration}s before running verify_instance on {name}")
    time.sleep(sleep_duration)
    super().verify_instance(name)

class NvidiaGpuDriverTestCase(AutomatedGpuTestCase):
  """
  Actual test suite for Nvidia GPU Driver initialization action.
  Inherits delays and other automated context from AutomatedGpuTestCase.
  """
  COMPONENT = "gpu"
  INIT_ACTIONS = ["gpu/install_gpu_driver.sh"]
  GPU_L4   = "type=nvidia-l4"
  GPU_T4   = "type=nvidia-tesla-t4"
  GPU_V100 = "type=nvidia-tesla-v100"
  GPU_A100 = "type=nvidia-tesla-a100,count=2"
  GPU_H100 = "type=nvidia-h100-80gb,count=2"

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

  def verify_instance_pyspark(self, name):
    # Verify that pyspark works
    self.assert_instance_command(name, "echo 'from pyspark.sql import SparkSession ; SparkSession.builder.getOrCreate()' | pyspark -c spark.executor.resource.gpu.amount=1 -c spark.task.resource.gpu.amount=0.01", 1)

  def verify_instance_cuda_version(self, name, cuda_version):
    self.assert_instance_command(
        name, "nvidia-smi -q -x | /opt/conda/default/bin/xmllint --xpath '//nvidia_smi_log/cuda_version/text()' - | grep {}".format(cuda_version) )

  def verify_instance_driver_version(self, name, driver_version):
    self.assert_instance_command(
        name, "nvidia-smi -q -x | /opt/conda/default/bin/xmllint --xpath '//nvidia_smi_log/driver_version/text()' - | grep {}".format(driver_version) )

  def verify_instance_spark(self):
    self.assert_dataproc_job(
      self.getClusterName(),
      "spark",
      "--jars=file:///usr/lib/spark/examples/jars/spark-examples.jar " \
      + "--class=org.apache.spark.examples.SparkPi " \
      + " -- 1000"
    )
    self.assert_dataproc_job(
      self.getClusterName(),
      "spark",
      "--jars=file:///usr/lib/spark/examples/jars/spark-examples.jar " \
      + "--class=org.apache.spark.examples.ml.JavaIndexToStringExample " \
      + "--properties="\
      +   "spark.executor.resource.gpu.amount=1,"\
      +   "spark.executor.cores=6,"\
      +   "spark.executor.memory=4G,"\
      +   "spark.plugins=com.nvidia.spark.SQLPlugin,"\
      +   "spark.executor.resource.gpu.discoveryScript=/usr/lib/spark/scripts/gpu/getGpusResources.sh,"\
      +   "spark.dynamicAllocation.enabled=false,"\
      +   "spark.sql.autoBroadcastJoinThreshold=10m,"\
      +   "spark.sql.files.maxPartitionBytes=512m,"\
      +   "spark.task.resource.gpu.amount=0.333,"\
      +   "spark.task.cpus=2,"\
      +   "spark.yarn.unmanagedAM.enabled=false"
    )
    self.assert_dataproc_job(
      self.getClusterName(),
      "spark",
      "--jars=file:///usr/lib/spark/examples/jars/spark-examples.jar " \
      + "--class=org.apache.spark.examples.ml.JavaIndexToStringExample " \
      + "--properties="\
      + "spark.driver.resource.gpu.amount=1,"\
      + "spark.driver.resource.gpu.discoveryScript=/usr/lib/spark/scripts/gpu/getGpusResources.sh,"\
      + "spark.executor.resource.gpu.amount=1,"\
      + "spark.executor.resource.gpu.discoveryScript=/usr/lib/spark/scripts/gpu/getGpusResources.sh"
    )

  def verify_driver_signature(self, name):
    cert_path='/var/lib/dkms/mok.pub'
    if self.getImageOs() == 'ubuntu':
      cert_path='/var/lib/shim-signed/mok/MOK.der'

    cert_verification_cmd = """
perl -Mv5.10 -e '
my $cert = ( qx{openssl x509 -inform DER -in {} -text}
             =~ /Serial Number:.*? +(.+?)\s*$/ms );
my $kmod = ( qx{modinfo nvidia}
             =~ /^sig_key:\s+(\S+)/ms );
exit 1 unless $cert eq lc $kmod
'
"""
    self.assert_instance_command( name, cert_verification_cmd.format(cert_path) )

  @parameterized.parameters(
      ("SINGLE", ["m"], GPU_T4, None, None),
  )
  def test_install_gpu_without_agent(self, configuration, machine_suffixes,
                                     master_accelerator, worker_accelerator,
                                     driver_provider):
#    if self.getImageOs() == 'rocky' and self.getImageVersion() >= pkg_resources.parse_version("2.2"):
#      self.skipTest("disabling rocky9 builds due to out of date base dataproc image")

    metadata = "install-gpu-agent=false"
    if configuration == 'SINGLE' \
    and self.getImageOs() == 'rocky' \
    and self.getImageVersion() <= pkg_resources.parse_version("2.1"):
      # ('2.1-rocky8 and 2.0-rocky8 tests are known to fail in SINGLE configuration with errors about nodes_include being empty')
      self.skipTest("known to fail")

    if driver_provider is not None:
      metadata += ",gpu-driver-provider={}".format(driver_provider)
    self.createCluster(
        configuration,
        self.INIT_ACTIONS,
        machine_type="n1-standard-16",
        master_accelerator=master_accelerator,
        worker_accelerator=worker_accelerator,
        metadata=metadata,
        timeout_in_minutes=90,
        boot_disk_size="50GB")
    for machine_suffix in machine_suffixes:
      machine_name="{}-{}".format(self.getClusterName(),machine_suffix)
      # Many failed nvidia-smi attempts have been caused by impatience and temporal collisions
      sleep_duration = 3 + random.randint(1, 30)
      print(f"Waiting for {sleep_duration}s before running verify_instance on {machine_name}")
      time.sleep(sleep_duration)
      self.verify_instance(machine_name)

  @parameterized.parameters(
      ("KERBEROS", ["m", "w-0", "w-1"], GPU_T4, GPU_T4, None),
#      ("STANDARD", ["w-0", "w-1"], None, GPU_T4, "NVIDIA"),
#      ("STANDARD", ["m"], GPU_T4, None, "NVIDIA"),
  )
  def test_install_gpu_with_agent(self, configuration, machine_suffixes,
                                  master_accelerator, worker_accelerator,
                                  driver_provider):
#    if self.getImageOs() == 'rocky' and self.getImageVersion() >= pkg_resources.parse_version("2.2"):
#      self.skipTest("disabling rocky9 builds due to out of date base dataproc image")

    self.skipTest("No need to regularly test installing the agent on its own cluster ; this is exercised elsewhere")

    if configuration == 'KERBEROS' \
    and self.getImageVersion() <= pkg_resources.parse_version("2.1"):
      # ('KERBEROS fails with image version <= 2.1')
      self.skipTest("known to fail")

    metadata = "install-gpu-agent=true"
    if driver_provider is not None:
      metadata += ",gpu-driver-provider={}".format(driver_provider)
    self.createCluster(
        configuration,
        self.INIT_ACTIONS,
        machine_type="n1-standard-16",
        master_accelerator=master_accelerator,
        worker_accelerator=worker_accelerator,
        metadata=metadata,
        timeout_in_minutes=90,
        boot_disk_size="50GB",
        scopes="https://www.googleapis.com/auth/monitoring.write")
    for machine_suffix in machine_suffixes:
      machine_name="{}-{}".format(self.getClusterName(),machine_suffix)
      self.verify_instance(machine_name)
      self.verify_instance_gpu_agent(machine_name)

  @parameterized.parameters(
        ("SINGLE", ["m"],               GPU_T4, None,   "12.4"),
#        ("SINGLE", ["m"],               GPU_T4, None,   "11.8"),
      ("STANDARD", ["m", "w-0", "w-1"], GPU_T4, GPU_T4, "12.4"),
      ("KERBEROS", ["m", "w-0", "w-1"], GPU_T4, GPU_T4, "11.8"),
  )
  def test_install_gpu_cuda_nvidia(self, configuration, machine_suffixes,
                                   master_accelerator, worker_accelerator,
                                   cuda_version):
#    if self.getImageOs() == 'rocky' and self.getImageVersion() >= pkg_resources.parse_version("2.2"):
#      self.skipTest("disabling rocky9 builds due to out of date base dataproc image")

    if configuration == 'KERBEROS' \
    and self.getImageVersion() <= pkg_resources.parse_version("2.1"):
      # ('KERBEROS fails with image version <= 2.1')
      self.skipTest("known to fail")

    if pkg_resources.parse_version(cuda_version) > pkg_resources.parse_version("12.4") \
    and ( ( self.getImageOs() == 'ubuntu' and self.getImageVersion() <= pkg_resources.parse_version("2.0") ) or \
          ( self.getImageOs() == 'debian' and self.getImageVersion() <= pkg_resources.parse_version("2.1") ) ):
      self.skipTest("CUDA > 12.4 not supported on older debian/ubuntu releases")

    if pkg_resources.parse_version(cuda_version) <= pkg_resources.parse_version("12.0") \
    and self.getImageVersion() >= pkg_resources.parse_version("2.2"):
      self.skipTest( "Kernel driver FTBFS with older CUDA versions on image version >= 2.2" )

    if configuration == 'SINGLE' \
    and self.getImageOs() == 'rocky' \
    and self.getImageVersion() <= pkg_resources.parse_version("2.1"):
      # ('2.1-rocky8 and 2.0-rocky8 tests are known to fail in SINGLE configuration with errors about nodes_include being empty')
      self.skipTest("known to fail")


    metadata = "gpu-driver-provider=NVIDIA,cuda-version={}".format(cuda_version)
    self.createCluster(
        configuration,
        self.INIT_ACTIONS,
        machine_type="n1-standard-16",
        master_accelerator=master_accelerator,
        worker_accelerator=worker_accelerator,
        metadata=metadata,
        timeout_in_minutes=90,
        boot_disk_size="50GB")

    for machine_suffix in machine_suffixes:
      machine_name="{}-{}".format(self.getClusterName(),machine_suffix)
      self.verify_instance(machine_name)
      self.verify_instance_nvcc(machine_name, cuda_version)
      self.verify_instance_pyspark(machine_name)
    self.verify_instance_spark()

  @parameterized.parameters(
      ("STANDARD", ["m"], GPU_H100, GPU_A100, "NVIDIA", "11.8"),
#      ("STANDARD", ["m"], GPU_H100, GPU_A100, "NVIDIA", "12.0"),
      ("STANDARD", ["m"], GPU_H100, GPU_A100, "NVIDIA", "12.4"),
  )
  def test_install_gpu_with_mig(self, configuration, machine_suffixes,
                                  master_accelerator, worker_accelerator,
                                  driver_provider, cuda_version):
#    if self.getImageOs() == 'rocky' and self.getImageVersion() >= pkg_resources.parse_version("2.2"):
#      self.skipTest("disabling rocky9 builds due to out of date base dataproc image")

    # Operation [projects/.../regions/.../operations/...] failed:
    # Invalid value for field 'resource.machineType': \
    # 'https://www.googleapis.com/compute/v1/projects/.../zones/.../' \
    # 'machineTypes/a3-highgpu-2g'. \
    # NetworkInterface NicType can only be set to GVNIC on instances with GVNIC GuestOsFeature..
    # ('This use case not thoroughly tested')
    self.skipTest("known to fail")

    if pkg_resources.parse_version(cuda_version) > pkg_resources.parse_version("12.4") \
    and ( ( self.getImageOs() == 'ubuntu' and self.getImageVersion() <= pkg_resources.parse_version("2.0") ) or \
          ( self.getImageOs() == 'debian' and self.getImageVersion() <= pkg_resources.parse_version("2.1") ) ):
      self.skipTest("CUDA > 12.4 not supported on older debian/ubuntu releases")

    if pkg_resources.parse_version(cuda_version) <= pkg_resources.parse_version("12.0") \
    and self.getImageVersion() >= pkg_resources.parse_version("2.2"):
      self.skipTest( "Kernel driver FTBFS with older CUDA versions on image version >= 2.2" )

    metadata = "gpu-driver-provider={},cuda-version={}".format(driver_provider, cuda_version)

    self.createCluster(
        configuration,
        self.INIT_ACTIONS,
        master_machine_type="a3-highgpu-2g",
        worker_machine_type="a2-highgpu-2g",
        master_accelerator=master_accelerator,
        worker_accelerator=worker_accelerator,
        metadata=metadata,
        timeout_in_minutes=90,
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
#    if self.getImageOs() == 'rocky' and self.getImageVersion() >= pkg_resources.parse_version("2.2"):
#      self.skipTest("disabling rocky9 builds due to out of date base dataproc image")

    if configuration == 'SINGLE' \
    and self.getImageOs() == 'rocky' \
    and self.getImageVersion() <= pkg_resources.parse_version("2.1"):
      # ('2.1-rocky8 and 2.0-rocky8 tests are known to fail in SINGLE configuration with errors about nodes_include being empty')
      self.skipTest("known to fail")

    metadata = None
    if driver_provider is not None:
      metadata = "gpu-driver-provider={}".format(driver_provider)

    self.createCluster(
        configuration,
        self.INIT_ACTIONS,
        metadata=metadata,
        machine_type="n1-standard-16",
        master_accelerator=master_accelerator,
        worker_accelerator=worker_accelerator,
        boot_disk_size="50GB",
        timeout_in_minutes=90)

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
#    if self.getImageOs() == 'rocky' and self.getImageVersion() >= pkg_resources.parse_version("2.2"):
#      self.skipTest("disabling rocky9 builds due to out of date base dataproc image")

    if pkg_resources.parse_version(cuda_version) > pkg_resources.parse_version("12.4") \
    and ( ( self.getImageOs() == 'ubuntu' and self.getImageVersion() <= pkg_resources.parse_version("2.0") ) or \
          ( self.getImageOs() == 'debian' and self.getImageVersion() <= pkg_resources.parse_version("2.1") ) ):
      self.skipTest("CUDA > 12.4 not supported on older debian/ubuntu releases")

    if pkg_resources.parse_version(cuda_version) <= pkg_resources.parse_version("12.0") \
    and self.getImageVersion() >= pkg_resources.parse_version("2.2"):
      self.skipTest( "Kernel driver FTBFS with older CUDA versions on image version >= 2.2" )

    if configuration == 'SINGLE' \
    and self.getImageOs() == 'rocky' \
    and self.getImageVersion() <= pkg_resources.parse_version("2.1"):
      # ('2.1-rocky8 and 2.0-rocky8 tests are known to fail in SINGLE configuration with errors about nodes_include being empty')
      self.skipTest("known to fail")

    metadata = "install-gpu-agent=true,gpu-driver-provider=NVIDIA,cuda-version={}".format(cuda_version)
    self.createCluster(
      configuration,
      self.INIT_ACTIONS,
      machine_type="n1-standard-16",
      master_accelerator=master_accelerator,
      worker_accelerator=worker_accelerator,
      metadata=metadata,
      timeout_in_minutes=90,
      boot_disk_size="50GB",
      scopes="https://www.googleapis.com/auth/monitoring.write")

    for machine_suffix in machine_suffixes:
      machine_name="{}-{}".format(self.getClusterName(),machine_suffix)
      self.verify_instance(machine_name)
      self.verify_instance_gpu_agent(machine_name)
    self.verify_instance_spark()

  @parameterized.parameters(
#    ("SINGLE", ["m"], GPU_T4, GPU_T4, "11.8", ''),
#    ("STANDARD", ["m"], GPU_T4, None, "12.0"),
#    ("STANDARD", ["m", "w-0", "w-1"], GPU_T4, GPU_T4, "11.8", 'rocky', '2.0'),
    ("STANDARD", ["m", "w-0", "w-1"], GPU_T4, GPU_T4, "12.4", 'rocky', '2.1'),
#    ("STANDARD", ["m", "w-0", "w-1"], GPU_T4, GPU_T4, "12.0", 'rocky', '2.2'),
#    ("KERBEROS", ["m", "w-0", "w-1"], GPU_T4, GPU_T4, "12.6", 'rocky', '2.2'),
#    ("STANDARD", ["w-0", "w-1"], None, GPU_T4, "11.8"),
#    ("STANDARD", ["w-0", "w-1"], None, GPU_T4, "12.0"),
  )
  def untested_driver_signing(self, configuration, machine_suffixes,
                           master_accelerator, worker_accelerator,
                           cuda_version, image_os, image_version):

    if configuration == 'KERBEROS' \
    and self.getImageVersion() <= pkg_resources.parse_version("2.1"):
      # ('KERBEROS fails with image version <= 2.1')
      self.skipTest("known to fail")

    kvp_array=[]
    import os

    if "private_secret_name" in os.environ:
      for env_var in ['public_secret_name', 'private_secret_name', 'secret_project', 'secret_version' 'modulus_md5sum']:
        kvp_array.append( "{}={}".format( env_var, os.environ[env_var] ) )

      if kvp_array[0] == "public_secret_name=":
        self.skipTest("This test only runs when signing environment has been configured in presubmit.sh")
    else:
      self.skipTest("This test only runs when signing environment has been configured in presubmit.sh")

    metadata = ",".join( kvp_array )

    if self.getImageOs() != image_os:
      self.skipTest("This test is only run on os {}".format(image_os))
    if self.getImageVersion() != image_version:
      self.skipTest("This test is only run on Dataproc Image Version {}".format(image_os))

    self.createCluster(
      configuration,
      self.INIT_ACTIONS,
      machine_type="n1-standard-16",
      master_accelerator=master_accelerator,
      worker_accelerator=worker_accelerator,
      metadata=metadata,
      timeout_in_minutes=90,
      boot_disk_size="50GB",
      scopes="https://www.googleapis.com/auth/monitoring.write")
    for machine_suffix in machine_suffixes:
      hostname="{}-{}".format(self.getClusterName(),machine_suffix)
      self.verify_instance(hostname)
      self.verify_instance_gpu_agent(hostname)
#      self.verify_driver_signature(hostname)

    self.verify_instance_spark()

if __name__ == "__main__":
  absltest.main()
