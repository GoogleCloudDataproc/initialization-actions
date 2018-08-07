#!/usr/bin/env python

import subprocess
import shlex


def fill_sql():
    mysql_commands = "CREATE DATABASE test;" \
                     "USE test;" \
                     "CREATE TABLE testtable(id INT, data VARCHAR(10));" \
                     "INSERT INTO testtable VALUES (0,\"value1\");" \
                     "INSERT INTO testtable VALUES (1,\"value2\");" \
                     "INSERT INTO testtable VALUES (2,\"value3\");" \
                     "INSERT INTO testtable VALUES (3,\"value4\");"
    subprocess.call(['mysql', '-u', 'root', '-e', mysql_commands])


def run_sqoop_import():
    sqoop_import_cmd = shlex.split('/usr/lib/sqoop/bin/sqoop import --connect jdbc:mysql://localhost/test '
                                   '--username root --table testtable --m 1')
    sqoop_import_out = subprocess.Popen(
        sqoop_import_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    info = sqoop_import_out.communicate()[0]
    if 'mapreduce.ImportJobBase: Retrieved 4 records.' not in info:
        raise Exception('Running sqoop import job failed.')


def main():
    fill_sql()
    run_sqoop_import()


if __name__ == '__main__':
    main()
