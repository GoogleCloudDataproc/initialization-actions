import os

import pkg_resources
from absl.testing import absltest
from absl.testing import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class HorovodTestCase(DataprocTestCase):
    COMPONENT = "horovod"
    CPU_INIT_ACTIONS = ["horovod/horovod.sh"]
    GPU_INIT_ACTIONS = ["gpu/install_gpu_driver.sh", "horovod/horovod.sh"]

    TENSORFLOW_TEST_SCRIPT = "scripts/verify_tensorflow.py"
    PYTORCH_TEST_SCRIPT = "scripts/verify_pytorch.py"
    MXNET_TEST_SCRIPT = "scripts/verify_mxnet.py"
    
    def _submit_spark_job(self, script):
       self.assert_dataproc_job(
            self.name, "pyspark", 
            "{}/horovod/scripts/{}".format(script, self.INIT_ACTIONS_REPO))
    
    @parameterized.parameters(
        ("STANDARD", "mpi"),
        ("STANDARD", "gloo"),
    )
    def test_horovod_cpu(self,configuration, controller):
        # Init action supported on Dataproc 1.3+
        if self.getImageVersion() < pkg_resources.parse_version("1.3"):
            return
        
        metadata=""
        if controller == "mpi":
            metadata+="install-mpi=true"
        
        self.createCluster(
            configuration,
            self.CPU_INIT_ACTIONS,
            timeout_in_minutes=30,
            machine_type="e2-standard-8",
            metadata=metadata)

    @parameterized.parameters(
        ("STANDARD", "mpi"),
        ("STANDARD","gloo"),
    )
    def test_horovod_gpu(self, configuration, controller):
        # Init action supported on Dataproc 1.3+
        if self.getImageVersion() < pkg_resources.parse_version("1.3"):
            return

        metadata=""
        if controller == "mpi":
            metadata+="install-mpi=true"
        
        self.createCluster(
            configuration,
            self.GPU_INIT_ACTIONS,
            timeout_in_minutes=60,
            machine_type="e2-standard-8",
            master_accelerator="nvidia-tesla-t4",
            worker_accelerator="nvidia-tesla-t4",
            metadata=metadata)


if __name__ == '__main__':
    absltest.main()
