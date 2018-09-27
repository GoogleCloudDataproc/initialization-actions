#!/usr/bin/env python

import subprocess


def create_commands_file():
    file = open("commands", 'w+')
    file.write("create 'test-bigtable', 'cf'\n"
               "list 'test-bigtable'\n"
               "put 'test-bigtable', 'row1', 'cf:a', 'value1'\n"
               "put 'test-bigtable', 'row2', 'cf:b', 'value2'\n"
               "put 'test-bigtable', 'row3', 'cf:c', 'value3'\n"
               "put 'test-bigtable', 'row4', 'cf:d', 'value4'\n"
               "exit"
               )
    file.close()


def main():
    create_commands_file()
    subprocess.check_output(['hbase', "shell", 'commands'])


if __name__ == '__main__':
    main()
