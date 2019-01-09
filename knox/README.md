# Knox

This [initialization action] (https://cloud.google.com/dataproc/init-actions) installs the latest version of [Knox] available in dataproc apt-get repository (http://knox.apache.org)
on a master node within a [Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster.

## Using this initialization action

You can use this initialization action to create a new Dataproc cluster with Knox installed:

1. Use the `gcloud` command to create a new cluster with this initialization action.  The following command will create a new cluster named `<CLUSTER_NAME>`.

    ```bash
    gcloud dataproc clusters create <CLUSTER_NAME> \
      --initialization-actions gs://dataproc-initialization-actions/knox/knox.sh
    ```

1. Once the cluster has been created, Knox is configured to run on port `8443` and `localhost` on the master node in a Dataproc cluster. You can test if it works with simple curl request:
```bash
curl -i -k -u guest:guest-password -X GET \
    'https://localhost:8443/gateway/sandbox/webhdfs/v1/?op=LISTSTATUS'
```

As a response you will see output like:
```bash
HTTP/1.1 200 OK
Content-Type: application/json
Content-Length: 760
Server: Jetty(6.1.26)

{"FileStatuses":{"FileStatus":[
{"accessTime":0,"blockSize":0,"group":"hdfs","length":0,"modificationTime":1350595859762,"owner":"hdfs","pathSuffix":"apps","permission":"755","replication":0,"type":"DIRECTORY"},
{"accessTime":0,"blockSize":0,"group":"mapred","length":0,"modificationTime":1350595874024,"owner":"mapred","pathSuffix":"mapred","permission":"755","replication":0,"type":"DIRECTORY"},
{"accessTime":0,"blockSize":0,"group":"hdfs","length":0,"modificationTime":1350596040075,"owner":"hdfs","pathSuffix":"tmp","permission":"777","replication":0,"type":"DIRECTORY"},
{"accessTime":0,"blockSize":0,"group":"hdfs","length":0,"modificationTime":1350595857178,"owner":"hdfs","pathSuffix":"user","permission":"755","replication":0,"type":"DIRECTORY"}
]}}
```

You can read more about it from official [Knox manual](https://knox.apache.org/books/knox-0-9-0/user-guide.html).

## Automated tests

Automated test included in ```test_knox.py``` file triggers basic check for Knox service. It checks whether file uploaded to hdfs via Knox can be also printed out using Knox API.
In order to run this test launch command from project top directory: 
```bash
python3 -m unittest knox.test_knox
```
