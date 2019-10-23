"""verify_tez.py: this script run tez job on node"""

import shlex
import subprocess


def main():
    copy_test_file_cmd = shlex.split(
        'hadoop fs -copyFromLocal /usr/lib/tez/LICENSE-MIT /tmp/LICENSE-MIT')
    copy_test_file_out = subprocess.Popen(
        copy_test_file_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    health_check_cmd = shlex.split(
        'hadoop jar /usr/lib/tez/tez-examples.jar orderedwordcount /tmp/LICENSE-MIT /tmp/tez-out'
    )
    health_check_out = subprocess.Popen(
        health_check_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    info = health_check_out.communicate()[0]
    if 'DAG completed. FinalState=SUCCEEDED' not in info:
        raise Exception('Running tez job failed.')


if __name__ == '__main__':
    main()
