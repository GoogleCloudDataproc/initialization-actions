"""
This module provides testing functionality of the Dataproc Metastore gRPC proxy
init action.

Test logic:
1. Create a gRPC-enabled Dataproc Metastore service.
2. Create a Dataproc cluster using the init action in order to access the
   metastore.
3. Run a Dataproc job to create a Hive table in the Dataproc Metastore service
   and verify that it is created in the metastore's warehouse directory.
"""
import json
import pkg_resources

from absl.testing import absltest
from absl.testing import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class MetastoreGrpcProxyTestCase(DataprocTestCase):
    COMPONENT = 'metastore-grpc-proxy'
    INIT_ACTIONS = ['metastore-grpc-proxy/metastore-grpc-proxy.sh']

    def __init__(self, method_name='runTest'):
        super().__init__(method_name)
        self.metastore_id = "test-{}-{}".format(self.datetime_str(),
                                                self.random_str())
        self.whd = None
        self.metadata = None
        self.properties = None

    def setUp(self):
        super().setUp()
        hive_version = ('2.3.6'
                        if self.getImageVersion() < pkg_resources.parse_version("2.0")
                        else '3.1.2')
        if self.getImageVersion() < pkg_resources.parse_version("1.5"):
          self.skipTest('Skipping unsupported image version: {}'.format(self.getImageVersion()))
        self.__assertMetastoreCreation(hive_version)

        _, endpoint_uri, _ = self.assert_command('gcloud beta metastore services describe --format "get(endpointUri)" --location %s %s' % (self.REGION, self.metastore_id))
        _, self.whd, _ = self.assert_command('gcloud beta metastore services describe --format "get(hiveMetastoreConfig.configOverrides[hive.metastore.warehouse.dir])" --location %s %s' % (self.REGION, self.metastore_id))
        self.whd = self.whd.strip()

        self.metadata = 'proxy-uri=%s,hive-version=%s' % (endpoint_uri.strip(), hive_version)
        self.properties = 'hive:hive.metastore.uris=thrift://localhost:9083,hive:hive.metastore.warehouse.dir=' + self.whd


    def tearDown(self):
        super().tearDown()
        self.assert_command(
            'gcloud metastore services delete --quiet --location {} {}'.format(self.REGION, self.metastore_id))

    @parameterized.parameters('SINGLE', 'STANDARD', 'HA')
    def test_metastore_grpc_proxy(self, configuration):
        self.createCluster(
            configuration,
            self.INIT_ACTIONS,
            scopes='https://www.googleapis.com/auth/cloud-platform',
            optional_components=['DOCKER'],
            metadata=self.metadata,
            properties=self.properties)
        table_name = '%s_%s' % (self.metastore_id.replace('-', '_'), configuration.lower())
        self.__create_test_table(table_name)
        self.__assertTableInWarehouseDir(table_name)


    def __create_test_table(self, table_name):
      self.assert_dataproc_job(
          self.getClusterName(),
          'spark-sql',
          '-e "CREATE TABLE %s (id int, name string)"' % table_name)

    def __assertTableInWarehouseDir(self, table_name):
      self.assert_command('gsutil stat %s/%s/' % (self.whd, table_name))


    def __assertMetastoreCreation(self, hive_version):
        _, dpms_create_stdout, _ = self.assert_command(
            'curl -H "Authorization: Bearer $(gcloud auth print-access-token)" '
            '-H "Content-Type: application/json" '
            '-H "Accept: application/json" '
            '-X POST '
            '-d \'{"tier": "DEVELOPER", '
                  '"hiveMetastoreConfig": {"endpointProtocol": "GRPC", '
                                          '"version": "%s"}}\' '
            '"https://metastore.googleapis.com/v1beta/projects/%s/locations/%s/services?serviceId=%s"' % (hive_version, self.PROJECT, self.REGION, self.metastore_id))
        self.__assertMetastoreOp(json.loads(dpms_create_stdout)['name'])

    def __assertMetastoreOp(self, op):
      while True:
        _, stdout, _ = DataprocTestCase.run_command('gcloud metastore operations describe --format="get(done)" ' + op)
        if 'true' in stdout.lower():
          break
        # Times out after 15 minutes, but LRO could take 25+ minutes
        DataprocTestCase.run_command('gcloud metastore operations wait ' + op)
      _, stdout, _ = DataprocTestCase.run_command('gcloud metastore operations describe --format="get(error)" ' + op)
      self.assertFalse(bool(stdout.strip()), msg='Metastore operation failed: ' + op)


if __name__ == '__main__':
    absltest.main()
