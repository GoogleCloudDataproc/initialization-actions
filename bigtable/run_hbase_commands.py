#!/usr/bin/env python

import subprocess
import datetime
import requests
import time

#import google.auth.transport.requests
import requests
import json

#from google.oauth2 import service_account
from google.cloud import bigtable
from google.cloud.bigtable import column_family
from google.cloud.bigtable import row_filters

#table_id='test-bigtable-'+str(int(time.time()))
table_id='test-bigtable-1695844085'

MDSV1_PFX='http://metadata.google.internal/computeMetadata/v1/'
INIT_ACTION_DOC_LINK='https://github.com/GoogleCloudDataproc/' + \
    'initialization-actions/tree/master/bigtable#using-this-initialization-action'

def create_commands_file():
    command_format = """
create '{}', 'cf'
list 'test-bigtable'
put 'test-bigtable', 'row1', 'cf:a', 'value1'
put 'test-bigtable', 'row2', 'cf:b', 'value2'
put 'test-bigtable', 'row3', 'cf:c', 'value3'
put 'test-bigtable', 'row4', 'cf:d', 'value4'
exit
"""
    with open("commands.txt", "w+") as file:
        file.write(command_format.format( table_id ))

def main():
    create_commands_file()
    subprocess.check_output(['hbase', "shell", 'commands.txt'])

    # install bigtable python client libraries
    # https://cloud.google.com/bigtable/docs/samples-python-hello#using-cloud-library
    subprocess.check_output(['python3', '-m', 'pip', 'install',
                             'google-cloud-bigtable==2.17.0',
                             'google-cloud-core==2.3.2'
                             ])

    # curl -H "Metadata-Flavor: Google" \
    #     http://metadata.google.internal/computeMetadata/v1/project/project-id
    s = requests.Session()
    s.headers.update({"Metadata-Flavor": "Google"})
    r = s.get(MDSV1_PFX + 'project/project-id')

    if r.status_code != 200:
        print("failed to request project ID")
        exit( 1 )

    project_id=r.text

    # curl -H "Metadata-Flavor: Google" \
    #     http://metadata.google.internal/.../instance/attributes/bigtable-instance
    r = s.get(MDSV1_PFX + 'instance/attributes/bigtable-instance')
    if r.status_code != 200:
        print( "failed to determine bigtable-instance attribute\n"+
               INIT_ACTION_DOC_LINK )
        exit( 1 )

    bigtable_instance=r.text

    # here we will verify that bigtable was updated
    # https://cloud.google.com/bigtable/docs/samples-python-hello

    # The client must be created with admin=True because it will
    # clean up the table hbase shell created

    client = bigtable.Client(project=project_id, admin=True)
    instance = client.instance(bigtable_instance)

    table = instance.table(table_id)

    if not table.exists():
        print("table does not exist")
        exit( 1 )
    else:
        print("table was created by hbase shell")

if __name__ == '__main__':
    main()
