import subprocess
import time
import sys

CLUSTER_NAME = sys.argv[1]
SAMPLE_H2O_JOB_FILE_NAME = sys.argv[2]


def run_command(command):
    p = subprocess.Popen(
        command,
        shell=True,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    stdout, stderr = p.communicate()
    stdout, stderr = stdout.decode('utf-8'), stderr.decode('utf-8')
    return p.returncode, stdout, stderr


def run_success(stdout, stderr):
    if ('SUCCESS' in stdout and 'finished successfully' in stdout) or ('SUCCESS' in stderr and 'finished successfully' in stderr):
        return True
    else:
        return False


def test_h2O_install_conda_pip():
    test_command_str = 'gcloud dataproc jobs submit pyspark --cluster {CLUSTER_NAME} --region global {SAMPLE_H2O_JOB_FILE_NAME}'
    retcode, stdout, stderr = run_command(test_command_str.format(CLUSTER_NAME=CLUSTER_NAME, SAMPLE_H2O_JOB_FILE_NAME=SAMPLE_H2O_JOB_FILE_NAME))
    assert retcode == 0, 'Sample H2O Sparkling Water Job failed. Return code: {} Error: {}'.format(retcode, stderr)
    if not run_success(stdout, stderr):
        raise Exception('Sample H2O Sparkling Water Job did not succeed.')

def main():
    test_h2O_install_conda_pip()


if __name__ == '__main__':
    main()
