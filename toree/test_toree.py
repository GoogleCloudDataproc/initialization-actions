# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pkg_resources

from absl.testing import absltest
from absl.testing import parameterized

from integration_tests.dataproc_test_case import DataprocTestCase


class ToreeTestCase(DataprocTestCase):
    COMPONENT = 'toree'
    INIT_ACTIONS = ['toree/toree.sh']

    @parameterized.parameters(
        ("SINGLE", "m", True),
        ("STANDARD", "m", True),
        ("HA", "m-0", True),
        ("SINGLE", "m", False),
        ("KERBEROS", "m", False),
    )
    def test_toree(self, configuration, machine_suffix, install_explicit):
        properties = "dataproc:jupyter.port=12345"
        if install_explicit:
            properties = "dataproc:pip.packages='toree==0.5.0',dataproc:jupyter.port=12345"
        optional_components = ["JUPYTER"]
        if self.getImageVersion() < pkg_resources.parse_version("2.0"):
            optional_components = ["ANACONDA", "JUPYTER"]
        self.createCluster(
            configuration,
            self.INIT_ACTIONS,
            optional_components=optional_components,
            properties=properties)
        instance_name = self.getClusterName() + "-" + machine_suffix
        _, stdout, _ = self.assert_instance_command(
            instance_name, "curl http://127.0.0.1:12345/api/kernelspecs")
        self.assertIn("Apache Toree", stdout)


if __name__ == '__main__':
    absltest.main()
