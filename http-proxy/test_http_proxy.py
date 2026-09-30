import logging
import os
import json
import pkg_resources
from absl.testing import absltest
from absl.testing import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class HttpProxyTestCase(DataprocTestCase):
  COMPONENT = 'http-proxy'
  INIT_ACTIONS = ['http-proxy/http-proxy.sh']
  SWP_AVAILABLE = False
  CA_CERT_URI = None

  @classmethod
  def setUpClass(cls):
    super().setUpClass()
    cls.SWP_AVAILABLE = False
    try:
      # Check SWP gateway
      ret, stdout, stderr = cls.run_command(
          f"gcloud network-services gateways list --location={cls.REGION} --project={cls.PROJECT} --format='value(name)'"
      )
      if ret == 0 and any('swp' in gw for gw in stdout.strip().split('\n') if gw):
        # Check CA Pool
        ret, stdout, stderr = cls.run_command(
            f"gcloud privateca pools list --location={cls.REGION} --project={cls.PROJECT} --format='value(name)'"
        )
        if ret == 0 and any('swp' in pool for pool in stdout.strip().split('\n') if pool):
          # Check Cert
          ret, stdout, stderr = cls.run_command(
              f"gcloud certificate-manager certificates list --location={cls.REGION} --project={cls.PROJECT} --format='value(name)'"
          )
          if ret == 0 and any('swp' in cert for cert in stdout.strip().split('\n') if cert):
            cls.SWP_AVAILABLE = True
    except Exception as e:
      logging.warning(f"Error checking SWP availability: {e}")

    if cls.SWP_AVAILABLE:
      try:
        cls.CA_CERT_URI = cls.setup_ca_cert()
      except Exception as e:
        logging.warning(f"Failed to setup CA cert: {e}")
        cls.SWP_AVAILABLE = False

  @classmethod
  def setup_ca_cert(cls):
    # 1. Find CA pool starting with swp-ca-pool-
    ret, stdout, stderr = cls.run_command(
        f"gcloud privateca pools list --location={cls.REGION} --project={cls.PROJECT} --format='value(name)'"
    )
    if ret != 0 or not stdout.strip():
      raise Exception(f"Failed to list CA pools: {stderr}")
    pools = stdout.strip().split('\n')
    swp_pools = [p for p in pools if 'swp' in p]
    if not swp_pools:
      raise Exception("No SWP CA pool found")
    pool_full_name = swp_pools[0]
    pool_name = pool_full_name.split('/')[-1]

    # 2. Find Root CA in this pool
    ret, stdout, stderr = cls.run_command(
        f"gcloud privateca roots list --pool={pool_name} --location={cls.REGION} --project={cls.PROJECT} --format='value(name)'"
    )
    if ret != 0 or not stdout.strip():
      raise Exception(f"Failed to list roots in pool {pool_name}: {stderr}")
    roots = stdout.strip().split('\n')
    swp_roots = [r for r in roots if 'swp' in r]
    if not swp_roots:
      raise Exception(f"No SWP root CA found in pool {pool_name}")
    root_full_name = swp_roots[0]
    root_name = root_full_name.split('/')[-1]

    # 3. Describe Root CA to get PEM
    ret, stdout, stderr = cls.run_command(
        f"gcloud privateca roots describe {root_name} --pool={pool_name} --location={cls.REGION} --project={cls.PROJECT} --format='value(pemCaCertificates)'"
    )
    if ret != 0 or not stdout.strip():
      raise Exception(f"Failed to get PEM for CA {root_name}: {stderr}")
    pem_cert = stdout.strip()

    # 4. Write to temp file and upload to GCS
    local_pem_path = "/tmp/swp-root-ca.pem"
    with open(local_pem_path, "w") as f:
      f.write(pem_cert)

    gcs_pem_uri = f"{cls.INIT_ACTIONS_REPO}/swp-root-ca.pem"
    ret, stdout, stderr = cls.run_command(f"gsutil cp {local_pem_path} {gcs_pem_uri}")
    if ret != 0:
      raise Exception(f"Failed to upload CA cert to GCS: {stderr}")

    return gcs_pem_uri

  @parameterized.parameters(
      ("SINGLE",),
  )
  def test_http_proxy_skip(self, configuration):
    # Test that it exits cleanly when no proxy metadata is provided
    self.createCluster(
        configuration,
        self.INIT_ACTIONS,
        timeout_in_minutes=10)

  @parameterized.parameters(
      ("SINGLE",),
  )
  def test_http_proxy_enabled(self, configuration):
    if not self.SWP_AVAILABLE:
      self.skipTest("SWP is not available/provisioned in this project")

    real_test_file = os.path.realpath(__file__)
    real_test_dir = os.path.dirname(real_test_file)
    repo_root_dir = os.path.dirname(real_test_dir)
    env_json_path = os.path.abspath(os.path.join(repo_root_dir, "env.json"))

    if not os.path.exists(env_json_path):
      self.skipTest(f"env.json not found at {env_json_path}. Skipping enabled proxy test.")

    with open(env_json_path, "r") as f:
      env_data = json.load(f)

    cluster_name = env_data["CLUSTER_NAME"]
    swp_ip = env_data["SWP_IP"]
    swp_port = env_data["SWP_PORT"]
    network = f"net-{cluster_name}"
    subnet = f"subnet-{cluster_name}"

    metadata = (
        f"http-proxy=http://{swp_ip}:{swp_port},"
        f"https-proxy=http://{swp_ip}:{swp_port},"
        f"http-proxy-pem-uri={self.CA_CERT_URI},"
        "no-proxy=metadata.google.internal,.googleapis.com"
    )

    self.createCluster(
        configuration,
        self.INIT_ACTIONS,
        metadata=metadata,
        network=network,
        subnet=subnet,
        timeout_in_minutes=15)

    # Verify internet connectivity through proxy
    self.assert_instance_command(
        f"{self.getClusterName()}-m",
        "curl -I -s --connect-timeout 10 https://www.google.com")


if __name__ == '__main__':
  absltest.main()
