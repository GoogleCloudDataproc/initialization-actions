#!/usr/bin/env python

import subprocess
import datetime
import requests

#import google.auth.transport.requests
import requests
import json

#from google.oauth2 import service_account
from google.cloud import bigtable
from google.cloud.bigtable import column_family
from google.cloud.bigtable import row_filters

def create_commands_file():
    command_string = """
create 'test-bigtable', 'cf'
list 'test-bigtable'
put 'test-bigtable', 'row1', 'cf:a', 'value1'
put 'test-bigtable', 'row2', 'cf:b', 'value2'
put 'test-bigtable', 'row3', 'cf:c', 'value3'
put 'test-bigtable', 'row4', 'cf:d', 'value4'
exit
"""
    with open("commands.txt", "w+") as file:
        file.write(command_string)

def main():
    create_commands_file()
#    subprocess.check_output(['hbase', "shell", 'commands.txt'])

    # install bigtable python client libraries
    # https://cloud.google.com/bigtable/docs/samples-python-hello#using-cloud-library
    subprocess.check_output(['python3', '-m', 'pip', 'install',
                             'google-cloud-bigtable==2.17.0',
                             'google-cloud-core==2.3.2'
                             ])

    # here we will verify that bigtable was updated
    # https://cloud.google.com/bigtable/docs/samples-python-hello
    # The client must be created with admin=True because it will create a
    # table.
#    scopes=['https://www.googleapis.com/auth/cloud-platform']

#    credentials = service_account.Credentials \
#                                 .with_scopes(scopes)
#    auth_req = google.auth.transport.requests.Request()
#    credentials.refresh(auth_req)

    s = requests.Session()
    s.headers.update({"Metadata-Flavor": "Google"})
    # curl -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/project/project-id
    r = s.get('http://metadata.google.internal/computeMetadata/v1/project/project-id')

    if r.status_code != 200:
        print("failed to request project ID")
        exit( 1 )
    client = bigtable.Client(project=r.text, admin=True)
    r = s.get('http://metadata.google.internal/computeMetadata/v1/instance/attributes/bigtable-instance')
    if r.status_code != 200:
        print("failed to request bigtable-instance attribute ; https://github.com/GoogleCloudDataproc/initialization-actions/tree/master/bigtable#using-this-initialization-action?")
        exit( 1 )
    instance = client.instance(r.text)


if __name__ == '__main__':
    main()
