#!/usr/bin/env python3
#
# Copyright 2019 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Create GPU metrics.

If you need to create metrics first run this script.
"""
import os

from google.cloud import monitoring_v3

GPU_UTILIZATION_METRIC_NAME = 'gpu_utilization'
GPU_MEMORY_UTILIZATION_METRIC_NAME = 'gpu_memory_utilization'


class MissingProjectIdError(Exception):
  pass


def add_new_metrics(project_id, metric_type, desc):
  """Add new Metrics for StackDriver.

  Args:
    project_id: (str) GCP project id.
    metric_type: (int) MetricDescriptor type.
    desc: (str) MetricDescriptor description.

  Raises:
    MissingProjectIdError: GCP Project id is not defined.
  """
  if not project_id:
    raise MissingProjectIdError(
        'Set the environment variable GCLOUD_PROJECT to your GCP Project ID.')
  descriptor = monitoring_v3.types.MetricDescriptor()
  descriptor.type = 'custom.googleapis.com/{type}'.format(type=metric_type)
  descriptor.metric_kind = (
      monitoring_v3.enums.MetricDescriptor.MetricKind.GAUGE)
  descriptor.value_type = (monitoring_v3.enums.MetricDescriptor.ValueType.INT64)
  descriptor.description = desc
  # Create Metric Descriptor.
  client = monitoring_v3.MetricServiceClient()
  project_name = client.project_path(project_id)
  descriptor = client.create_metric_descriptor(project_name, descriptor)
  print('Created {}.'.format(descriptor.name))


def main():
  # Get Project id information.
  project_id = (
      os.environ.get('GOOGLE_CLOUD_PROJECT') or
      os.environ.get('GCLOUD_PROJECT'))

  add_new_metrics(project_id, GPU_UTILIZATION_METRIC_NAME,
                  'Metric for GPU utilization.')
  add_new_metrics(project_id, GPU_MEMORY_UTILIZATION_METRIC_NAME,
                  'Metric for GPU memory utilization.')


if __name__ == '__main__':
  main()