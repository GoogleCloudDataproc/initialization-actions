#!/usr/bin/env python
import subprocess
def create_commands_file():
    with open("commands.sql", "w+") as file:
        file.write("use default;\n"
                    "show tables;\n"
                    "drop table if exists test;\n"
                    "create table test (a int, b string) STORED AS ORC TBLPROPERTIES (\"transactional\"=\"true\");\n"
                    "insert into test (a,b) values (1, \"hello\");\n"
                    "insert into test (a,b) values (2, \"world\");\n"
                    "update test set a=100 where b=\"world\";\n"
                    "select * from test where a=100;\n"
                    "!quit")
def main():
    create_commands_file()
    hostname=subprocess.check_output(['hostname']).decode('UTF-8').replace("\n","")
    beeline_connection='jdbc:hive2://'+ hostname +':10000/default'
    subprocess.check_output(['beeline', '-u', beeline_connection, '-f' ,'commands.sql'])

if __name__ == '__main__':
    main()