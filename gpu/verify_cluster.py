import argparse
import os
import time
from gpu.gpu_test_case_base import GpuTestCaseBase

import json

class VerifyCluster(GpuTestCaseBase):
    def __init__(self, cluster_name, region, zone):
        super().__init__()
        self.cluster_name = cluster_name
        self.name = cluster_name  # Set self.name for DataprocTestCase methods
        self.REGION = region  # Set REGION for DataprocTestCase
        self.cluster_region = region
        self.cluster_zone = zone
        # Mock other necessary DataprocTestCase attributes if needed
        self.datetime = time.strftime("%Y%m%d-%H%M%S")
        self.random = self.random_str(4)

    def check_cluster_status(self):
        print(f"--- Checking status of cluster {self.getClusterName()} ---")
        cmd = "gcloud dataproc clusters describe {} --region={} --format=json".format(
            self.getClusterName(), self.cluster_region)
        ret_code, stdout, stderr = self.run_command(cmd)

        if ret_code != 0:
            print(f"ERROR: Failed to get cluster status for {self.getClusterName()}.")
            print(f"STDERR: {stderr}")
            exit(1)

        try:
            cluster_info = json.loads(stdout)
            status = cluster_info.get('status', {}).get('state')
            if status == 'RUNNING':
                print(f"Cluster {self.getClusterName()} is RUNNING.")
            else:
                print(f"ERROR: Cluster {self.getClusterName()} is not in RUNNING state. Current state: {status}")
                exit(1)
        except json.JSONDecodeError:
            print(f"ERROR: Failed to parse cluster describe output as JSON.")
            print(f"STDOUT: {stdout}")
            exit(1)

    def verify_instance_spark(self):
        print(f"--- Verifying Spark on {self.getClusterName()} ---")
        ret_code, stdout, stderr = self.run_dataproc_job(
            self.getClusterName(),
            "spark",
            f"--region={self.cluster_region} "
            "--class=org.apache.spark.examples.SparkPi "
            "--jars=file:///usr/lib/spark/examples/jars/spark-examples.jar "
            "-- 10"  # Reduced iterations for faster test
        )

        if ret_code != 0:
            if "is in state ERROR and cannot accept jobs" in stderr:
                print(f"SPARK JOB FAILED: Cluster {self.getClusterName()} is in ERROR state. Agent on master may be down.")
                # Optionally print a more detailed guide
                print("  Check: systemctl status google-dataproc-agent on the master node.")
                print("  Logs: journalctl -u google-dataproc-agent on the master node.")
            else:
                print(f"SPARK JOB FAILED: ret_code={ret_code}")
                print(f"STDOUT:\n{stdout}")
                print(f"STDERR:\n{stderr}")
            raise AssertionError("SparkPi job failed on cluster")
        else:
            print(f"OK: SparkPi on {self.getClusterName()}")
        # Add other spark jobs from test_gpu.py if needed


    def get_instance_names(self):
        ret_code, stdout, stderr = self.run_command(
            "gcloud compute instances list --filter='name ~ {}-' --format='value(name)' --project={}".format(
                self.getClusterName(), self.getProjectId()))
        if ret_code != 0:
            print(stderr)
            raise Exception("Failed to list instances")
        return stdout.strip().split('\n')

    def getProjectId(self):
        # Assuming gcloud is configured
        ret_code, stdout, stderr = self.run_command("gcloud config get-value project")
        if ret_code != 0:
            print(stderr)
            raise Exception("Failed to get project ID")
        return stdout.strip()

def main():
    parser = argparse.ArgumentParser(description='Verify GPU setup on a running Dataproc cluster.')
    parser.add_argument('--cluster', default=os.environ.get('CLUSTER_NAME'), help='The name of the Dataproc cluster.')
    parser.add_argument('--region', default=os.environ.get('REGION'), help='The region of the cluster.')
    parser.add_argument('--zone', default=os.environ.get('ZONE'), help='The zone of the cluster.')
    parser.add_argument('--tests', nargs='+', default=['smi', 'agent', 'spark', 'torch'], help='Tests to run: smi, agent, spark, torch')
    args = parser.parse_args()

    if not args.cluster:
        parser.error("The --cluster argument is required if CLUSTER_NAME environment variable is not set.")
    if not args.region:
        parser.error("The --region argument is required if REGION environment variable is not set.")
    if not args.zone:
        parser.error("The --zone argument is required if ZONE environment variable is not set.")

    verifier = VerifyCluster(args.cluster, args.region, args.zone)
    verifier.check_cluster_status()

    instance_names = verifier.get_instance_names()
    print(f"Found instances: {instance_names}")

    if not instance_names or all(not s for s in instance_names):
        print(f"ERROR: No instances found for cluster '{args.cluster}'.")
        print("  Please check the following:")
        print("  1. Is the CLUSTER_NAME environment variable or --cluster argument correct?")
        print("  2. Is gcloud authenticated? Run 'gcloud auth list'.")
        print("  3. Is the correct project selected? Run 'gcloud config list'.")
        print("  4. Does the cluster actually exist and is running?")
        exit(1)

    for instance_name in instance_names:
        if 'smi' in args.tests:
            verifier.verify_instance(instance_name)
        if 'agent' in args.tests:
            verifier.verify_instance_gpu_agent(instance_name)
        if 'torch' in args.tests:
            verifier.verify_pytorch(instance_name)
        # Add other verify functions here

    if 'spark' in args.tests:
        verifier.verify_instance_spark()

    print("--- Verification Complete ---")

if __name__ == '__main__':
    main()
