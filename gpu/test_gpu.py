import pkg_resources
import time

from absl.testing import absltest
from absl.testing import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase

DEFAULT_TIMEOUT = 15  # minutes
DEFAULT_CUDA_VERSION = "12.4"

class NvidiaGpuDriverTestCase(DataprocTestCase):
  COMPONENT = "gpu"
  INIT_ACTIONS = ["gpu/install_gpu_driver.sh"]
  GPU_L4   = "type=nvidia-l4"
  GPU_T4   = "type=nvidia-tesla-t4"
  GPU_V100 = "type=nvidia-tesla-v100"
  GPU_A100 = "type=nvidia-tesla-a100,count=2"
  GPU_H100 = "type=nvidia-h100-80gb,count=2"

  # Tests for PyTorch
  TORCH_TEST_SCRIPT_FILE_NAME = "verify_pytorch.py"

  # Tests for TensorFlow
  TF_TEST_SCRIPT_FILE_NAME = "verify_tensorflow.py"

  def assert_instance_command(self,
                             instance,
                             cmd,
                             timeout_in_minutes=DEFAULT_TIMEOUT):

    retry_count = 5

    ssh_cmd='gcloud compute ssh -q {} --zone={} --command="{}" -- -o ConnectTimeout=60'.format(
      instance, self.cluster_zone, cmd)

    while retry_count > 0:
      try:
        ret_code, stdout, stderr = self.assert_command( ssh_cmd, timeout_in_minutes )
        return ret_code, stdout, stderr
      except Exception as e:
        print("An error occurred: ", e)
        retry_count -= 1
        if retry_count > 0:
          time.sleep(10)
          continue
        else:
          raise

  def verify_instance(self, name):
    # Verify that nvidia-smi works
    import random
    # Many failed nvidia-smi attempts have been caused by impatience and temporal collisions
    time.sleep( 3 + random.randint(1, 30) )
    self.assert_instance_command(name, "nvidia-smi", 1)

  def verify_pyspark(self, name):
    # Verify that pyspark works
    self.assert_instance_command(name, "echo 'from pyspark.sql import SparkSession ; SparkSession.builder.getOrCreate()' | pyspark -c spark.executor.resource.gpu.amount=1 -c spark.task.resource.gpu.amount=0.01", 1)

  def verify_pytorch(self, name):
    test_filename=os.path.join(os.path.dirname(os.path.abspath(__file__)),
                               self.TORCH_TEST_SCRIPT_FILE_NAME)
    self.upload_test_file(test_filename, name)

    conda_env="dpgce"
    verify_cmd = \
      "env={} ; envpath=/opt/conda/miniconda3/envs/${env} ; ".format(conda_env) + \
      "for f in $(ls /sys/module/nvidia/drivers/pci:nvidia/*/numa_node) ; do echo 0 > ${f} ; done ;" + \
      "${envpath}/bin/python {}".format(
        self.TORCH_TEST_SCRIPT_FILE_NAME)
    self.assert_instance_command(name, verify_cmd)
    self.remove_test_script(self.TORCH_TEST_SCRIPT_FILE_NAME, name)

  def verify_tensorflow(self, name):
    test_filename=os.path.join(os.path.dirname(os.path.abspath(__file__)),
                               self.TF_TEST_SCRIPT_FILE_NAME)
    self.upload_test_file(test_filename, name)
    # all on a single numa node
    verify_cmd = \
      "env={} ; envpath=/opt/conda/miniconda3/envs/${env} ; ".format("dpgce") + \
      "for f in $(ls /sys/module/nvidia/drivers/pci:nvidia/*/numa_node) ; do echo 0 > ${f} ; done ;" + \
      "${envpath}/bin/python {}".format(
        self.TF_TEST_SCRIPT_FILE_NAME)
    self.assert_instance_command(name, verify_cmd)
    self.remove_test_script(self.TF_TEST_SCRIPT_FILE_NAME, name)

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
      + "--class=org.apache.spark.examples.ml.JavaIndexToStringExample " \
      + "--properties="                           \
      +   "spark.executor.resource.gpu.amount=1," \
      +   "spark.executor.cores=6,"               \
      +   "spark.executor.memory=4G,"             \
      +   "spark.task.resource.gpu.amount=0.333," \
      +   "spark.task.cpus=2,"                    \
      +   "spark.yarn.unmanagedAM.enabled=false"
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
      ("SINGLE",   ["m"], GPU_T4, None, None),
#      ("STANDARD", ["m"], GPU_T4, None, None),
      ("STANDARD", ["m", "w-0", "w-1"], GPU_T4, GPU_T4, "NVIDIA"),
  )
  def test_install_gpu_default_agent(self, configuration, machine_suffixes,
                                     master_accelerator, worker_accelerator,
                                     driver_provider):
    self.skipTest("No need to regularly test installing the agent on its own cluster ; this is exercised elsewhere")

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
        machine_type="n1-highmem-32",
        master_accelerator=master_accelerator,
        worker_accelerator=worker_accelerator,
        metadata=metadata,
        timeout_in_minutes=90, # This cluster is sized and timed correctly to build the driver and nccl
        boot_disk_size="60GB")
    for machine_suffix in machine_suffixes:
      machine_name="{}-{}".format(self.getClusterName(),machine_suffix)
      self.verify_instance(machine_name)
      self.verify_instance_nvcc(machine_name, DEFAULT_CUDA_VERSION)
      self.verify_instance_pyspark(machine_name)
      self.verify_instance_spark()

  @parameterized.parameters(
      ("SINGLE", ["m"], GPU_T4, None, None),
  )
  def test_install_gpu_without_agent(self, configuration, machine_suffixes,
                                     master_accelerator, worker_accelerator,
                                     driver_provider):
    self.skipTest("No need to regularly test not installing the agent")

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
        machine_type="n1-highmem-8",
        master_accelerator=master_accelerator,
        worker_accelerator=worker_accelerator,
        metadata=metadata,
        timeout_in_minutes=90,
        boot_disk_size="50GB")
    for machine_suffix in machine_suffixes:
      machine_name="{}-{}".format(self.getClusterName(),machine_suffix)
      self.verify_instance(machine_name)

  @parameterized.parameters(
      ("KERBEROS", ["m", "w-0", "w-1"], GPU_T4, GPU_T4, None),
#      ("STANDARD", ["w-0", "w-1"], None, GPU_T4, "NVIDIA"),
#      ("STANDARD", ["m"], GPU_T4, None, "NVIDIA"),
  )
  def test_install_gpu_with_agent(self, configuration, machine_suffixes,
                                  master_accelerator, worker_accelerator,
                                  driver_provider):
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
        machine_type="n1-highmem-8",
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

    if ( self.getImageOs() == 'rocky' ) and self.getImageVersion() >= pkg_resources.parse_version("2.2"):
      self.skipTest("GPU drivers are currently FTBFS on Rocky 9 ; base dataproc image out of date")

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
        machine_type="n1-highmem-8",
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

    if ( self.getImageOs() == 'rocky' ) and self.getImageVersion() >= pkg_resources.parse_version("2.2"):
      self.skipTest("GPU drivers are currently FTBFS on Rocky 9 ; base dataproc image out of date")

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
        machine_type="n1-highmem-8",
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

    if ( self.getImageOs() == 'rocky' ) and self.getImageVersion() >= pkg_resources.parse_version("2.2"):
      self.skipTest("GPU drivers are currently FTBFS on Rocky 9 ; base dataproc image out of date")

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
      machine_type="n1-highmem-8",
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

    if ( self.getImageOs() == 'rocky' ) and self.getImageVersion() >= pkg_resources.parse_version("2.2"):
      self.skipTest("GPU drivers are currently FTBFS on Rocky 9 ; base dataproc image out of date")

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
      machine_type="n1-highmem-8",
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
