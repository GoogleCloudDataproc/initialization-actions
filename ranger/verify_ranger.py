"""verify_ranger.py: this script runs tests on node running ranger admin

Test logic:
1. Testing ranger admin UI:
Default ranger admin port is set to 6080.
This test validates if ranger admin is available on this port.

2. Testing hdfs.
By default user has permissions to list content of its user directory on hdfs.
This test case creates test directory in /user/username hdfs dir and then
creates policy which denies read access on that test directory for this
particular user. Test validates if policy works by running hdfs dfs -ls command.

3. Testing hive.
By default ranger on dataproc come with the hive service which defines some
policies. Default policy gives admin user full recursive access over all
resources in hive server. This test creates new database and new empty table
that has all permissions granted to admin user. Then it creates a policy that
will deny using 'SELECT' for 'admin' user on this empty table. Test validates
if policy works by running hive query using beeline tool.

4. Testing yarn.
By default dataproc user has permission to submit application using yarn
command. This test case creates yarn service policy which denies submitting
applications by default user. Tests validates if policy works by submitting
example mapreduce job using yarn jar command.

Note: It takes up to 30s for ranger to refresh policy settings.
"""

import subprocess
import json

RANGER_POLICY_REFRESH_TIME = 30


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


def test_ranger_admin():
    test_command = 'curl -L http://localhost:6080'
    ret_code, stdout, stderr = run_command(test_command)
    assert ret_code == 0, \
        'Failed to connect to Ranger Admin UI. Return code: {} Error: {}'.format(ret_code, stderr)
    if '<title> Ranger - Sign In</title>' not in stdout:
        raise Exception('Ranger Admin UI is not working properly.')


def test_hdfs_plugin():
    _, username, _ = run_command('whoami')
    policy = """
    {
    "allowExceptions": [],
    "denyExceptions": [],
    "denyPolicyItems": [
        {
            "accesses": [
                {
                    "isAllowed": true,
                    "type": "read"
                }
            ],
            "conditions": [],
            "delegateAdmin": false,
            "groups": [],
            "users": []
        }
    ],
    "description": "Policy for Service: hadoop-dataproc",
    "isAuditEnabled": true,
    "isEnabled": true,
    "name": "hdfs-access-restrictions",
    "policyItems": [],
    "resources": {
        "path": {
            "isExcludes": false,
            "isRecursive": true,
            "values": []
        }
    },
    "service": "hadoop-dataproc",
    "version": 1
    }"""
    policy_json = json.loads(policy)
    policy_json['denyPolicyItems'][0]['users'].append(username.strip())
    policy_json['resources']['path']['values'].append('/user/{}/test'.format(
        username.strip()))

    create_dir = 'hdfs dfs -mkdir -p /user/{}/test'.format(username.strip())
    ret_code, stdout, stderr = run_command(create_dir)
    assert ret_code == 0, \
        'Failed to create test hdfs directory. Return code: {} Error: {}'.format(ret_code, stderr)

    create_policy = ("curl --user \"admin:{}\" -H \"Content-Type: "
                     "application/json\" -X POST -d '{}' "
                     'http://localhost:{}/service/public/v2/api/policy')\
        .format('dataproc2019', json.dumps(policy_json), 6080)
    ret_code, stdout, stderr = run_command(create_policy)
    if '"isEnabled":true,"createdBy":"Admin"' not in stdout:
        raise Exception(
            'Failed to create hdfs policy. Return code: {} Error: {}'.format(
                ret_code, stdout))

    validate_policy = 'sleep {} && hdfs dfs -ls /user/{}/test'\
        .format(RANGER_POLICY_REFRESH_TIME, username.strip())
    ret_code, stdout, stderr = run_command(validate_policy)
    validation_string = ('Permission denied: user={}, access=READ_EXECUTE, '
                         'inode="/user/{}/test"')\
        .format(username.strip(), username.strip())
    if validation_string not in stderr:
        raise Exception('Ranger hdfs plugin is not working properly.')


def test_hive_plugin():
    policy = """
    {
    "allowExceptions": [],
    "denyExceptions": [],
    "denyPolicyItems": [
        {
            "accesses": [
                {
                    "isAllowed": true,
                    "type": "select"
                }
            ],
            "conditions": [],
            "delegateAdmin": true,
            "groups": [],
            "users": []
        }
    ],
    "description": "Policy for Service: hive-dataproc",
    "isAuditEnabled": true,
    "isEnabled": true,
    "name": "hive-access-restrictions",
    "policyItems": [],
    "resources": {
        "database": {
            "values": [
                "ranger_test_db"
            ]
        },
        "table": {
            "values": [
                "*"
            ]
        },
        "column": {
            "values": [
                "*"
            ]
        }
    },
    "service": "hive-dataproc",
    "version": 1
    }
    """
    policy_json = json.loads(policy)
    policy_json['denyPolicyItems'][0]['users'].append('admin')
    create_policy = ("curl --user \"admin:{}\" -H \"Content-Type: "
                     "application/json\" -X POST -d '{}' "
                     'http://localhost:{}/service/public/v2/api/policy') \
        .format('dataproc2019', json.dumps(policy_json), 6080)
    ret_code, stdout, stderr = run_command(create_policy)
    if '"isEnabled":true,"createdBy":"Admin"' not in stdout:
        raise Exception(
            'Failed to create hive policy. Return code: {} Error: {}'.format(
                ret_code, stdout))

    create_table = 'hive -e \'create database ranger_test_db; ' \
                   'use ranger_test_db; create table ranger_test_table(id int);\''
    ret_code, stdout, stderr = run_command(create_table)
    assert ret_code == 0, \
        'Failed to create test database or table. Return code: {} Error: {}'.format(ret_code, stderr)
    validate_policy = ('sleep {} && beeline -u jdbc:hive2://localhost:10000 -n '
                       'admin -e \'select * from '
                       'ranger_test_db.ranger_test_table;\'')\
        .format(RANGER_POLICY_REFRESH_TIME)

    ret_code, stdout, stderr = run_command(validate_policy)
    validation_string = \
                        ('FAILED: HiveAccessControlException Permission denied: '
                         'user [admin] does not have [SELECT] privilege')
    if validation_string not in stderr:
        raise Exception('Ranger hive plugin is not working properly')


def test_yarn_plugin():
    _, username, _ = run_command('whoami')
    policy = """
    {
    "allowExceptions": [],
    "denyExceptions": [],
    "denyPolicyItems": [
        {
            "accesses": [
                {
                    "isAllowed": true,
                    "type": "submit-app"
                },
                {
                    "isAllowed": true,
                    "type": "admin-queue"
                }
            ],
            "conditions": [],
            "delegateAdmin": true,
            "groups": [],
            "users": []
        }
    ],
    "description": "Policy for Service: yarn-dataproc",
    "isAuditEnabled": true,
    "isEnabled": true,
    "name": "yarn-access-restrictions",
    "policyItems": [],
    "resources": {
        "queue": {
            "values": [
                "root.default"
            ]
        }
    },
    "service": "yarn-dataproc",
    "version": 1
    }
    """
    policy_json = json.loads(policy)
    policy_json['denyPolicyItems'][0]['users'].append(username.strip())
    create_policy = ("curl --user \"admin:{}\" -H \"Content-Type: "
                     "application/json\" -X POST -d '{}' "
                     'http://localhost:{}/service/public/v2/api/policy') \
        .format('dataproc2019', json.dumps(policy_json), 6080)
    ret_code, stdout, stderr = run_command(create_policy)
    if '"isEnabled":true,"createdBy":"Admin"' not in stdout:
        raise Exception(
            'Failed to create yarn policy. Return code: {} Error: {}'.format(
                ret_code, stdout))
    validate_policy = \
                      ('sleep {} && find /usr/lib/hadoop-mapreduce -name '
                       'hadoop-mapreduce-examples-* -exec yarn jar {{}} pi 16 '
                       '1000 \;')\
        .format(RANGER_POLICY_REFRESH_TIME)
    ret_code, stdout, stderr = run_command(validate_policy)

    validation_string = 'org.apache.hadoop.security.AccessControlException: ' \
                        'User {} cannot submit applications to queue root.default'\
        .format(username.strip())
    validation_string_deb_package = ('org.apache.hadoop.security.AccessControlException:'
                                     ' User {} does not have permission to '
                                     'submit application') \
        .format(username.strip())
    if (validation_string not in stderr) and (
            validation_string_deb_package not in stderr):
        raise Exception('Ranger yarn plugin is not working properly')


def main():
    test_ranger_admin()
    test_hdfs_plugin()
    test_hive_plugin()
    test_yarn_plugin()


if __name__ == '__main__':
    main()
