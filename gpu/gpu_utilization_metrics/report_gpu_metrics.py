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
"""Report GPU metrics.

Installs a monitoring agent that monitors the GPU usage on the instance.
This will auto create the GPU metrics.
"""

import csv
import requests
import subprocess
import time

from google.cloud import monitoring_v3


GPU_UTILIZATION_METRIC_NAME = 'gpu_utilization'
GPU_MEMORY_UTILIZATION_METRIC_NAME = 'gpu_memory_utilization'
METADATA_SERVER = 'http://metadata/computeMetadata/v1/instance/'
METADATA_FLAVOR = {'Metadata-Flavor': 'Google'}
SLEEP_TIME = 15


def report_metric(client, value, metric_type, instance_id, zone, project_id):
  """Create time series for report.

  Args:
    client: (monitoring_v3.MetricServiceClient()) A Metric Service.
    value: (int) Report metric value.
    metric_type: (str) Metric type
    instance_id: (str) Instance id of Compute resource.
    zone: (str) Compute Zone.
    project_id: (str) Project Identifier from GCP.
  """
  project_name = client.project_path(project_id)
  series = monitoring_v3.types.TimeSeries()
  series.metric.type = 'custom.googleapis.com/{type}'.format(type=metric_type)
  series.resource.type = 'gce_instance'
  series.resource.labels['instance_id'] = instance_id
  series.resource.labels['zone'] = zone
  series.resource.labels['project_id'] = project_id
  point = series.points.add()
  point.value.int64_value = value
  now = time.time()
  point.interval.end_time.seconds = int(now)
  point.interval.end_time.nanos = int(
      (now - point.interval.end_time.seconds) * 10**9)
  client.create_time_series(project_name, [series])


def get_nvidia_smi_utilization(gpu_query_name):
  """Obtain NVIDIA SMI GPU utilization.

  Args:
    gpu_query_name: (str) GPU query name.

  Returns:
    An `int` of smi utilization.
  """
  csv_file_path = '/tmp/gpu_utilization.csv'
  usage = 0
  length = 0
  subprocess.check_call([
      '/bin/bash', '-c', 'nvidia-smi --query-gpu={query_name} -u --format=csv'
      ' > {csv_file_path}'.format(
          query_name=gpu_query_name, csv_file_path=csv_file_path)
  ])
  with open(csv_file_path) as csvfile:
    utilizations = csv.reader(csvfile, delimiter=' ')
    for row in utilizations:
      length += 1
      if length > 1:
        usage += int(row[0])
  return int(usage / (length - 1))


def get_gpu_utilization():
  return get_nvidia_smi_utilization('utilization.gpu')


def get_gpu_memory_utilization():
  return get_nvidia_smi_utilization('utilization.memory')


def main():
  # Get instance information
  data = requests.get(METADATA_SERVER + 'zone', headers=METADATA_FLAVOR).text
  instance_id = requests.get(
      METADATA_SERVER + 'id', headers=METADATA_FLAVOR).text
  # Collect zone
  zone = data.split('/')[3]
  # Collect project id
  project_id = data.split('/')[1]
  # Report metrics loop.
  client = monitoring_v3.MetricServiceClient()
  while True:
    report_metric(client, get_gpu_utilization(), GPU_UTILIZATION_METRIC_NAME,
                  instance_id, zone, project_id)
    report_metric(client, get_gpu_memory_utilization(),
                  GPU_MEMORY_UTILIZATION_METRIC_NAME, instance_id, zone,
                  project_id)
    time.sleep(SLEEP_TIME)


if __name__ == '__main__':
  main()