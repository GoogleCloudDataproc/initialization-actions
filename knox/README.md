# Apache Knox Initialization Action

This [initialization action](https://cloud.google.com/dataproc/init-actions)
installs [Apache Knox](https://knox.apache.org/) on the first master node within
a [Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster. The Knox
distributed by the Bigtop project is installed via `apt-get` so it may not be
the latest Knox.

Moreover, initialization action configures the knox gateway by:

-   adding topologies for backend clusters
-   adding or creating TLS certificates for HTTPS
-   setting the master key of Knox
-   altering the main configuration file, *gateway-site.xml*
-   adds a cron schedule to update the configuration from a GCS bucket
    periodically

### Caveats

-   Dataproc Jobs API uses binary transport protocol of Hive. When we switch the
    protocol to HTTP for Knox, you cannot use the Jobs API for these backend
    clusters. See [Additional setup for Hive](#additional-setup-for-hive) for
    details.
-   [Component Gateway](https://cloud.google.com/dataproc/docs/concepts/accessing/dataproc-gateways)
    does not work for the gateway cluster when *SSL* is enabled by changing
    [gateway-site.xml](./gateway-site.xml) or when
    [default.xml](./topologies/default.xml) is altered since component gateway
    also uses Knox. Note that, the backend clusters that gateway redirects can
    still use the component gateway.

## Using this initialization action

**:warning: NOTICE:** See
[best practices](/README.md#how-initialization-actions-are-used) of using
initialization actions in production.

This initialization action requires a bucket that stores configuration. By
providing a bucket with configurations, you can create a Dataproc cluster with
Knox installed as follows:

1.  Use the `gcloud` command to create a new cluster with this initialization
    action.

    ```bash
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION}
        --initialization-actions gs://dataproc-initialization-actions-${REGION}/knox/knox.sh \
        --metadata knox-gw-config=gs://<GCS path to your Knox configuration directory>
    ```

    The configuration bucket should be a modified copy of this git directory.
    The bucket structure is:

    ```
     <your GCS path>
       |
       |- gateway-site.xml  --> main gateway configuration file.
       |- knox-config.yaml  --> variables to setup master key and certificates.
                                You may set the variables by passing them as metadata as well.
                                Refer to the config file for variables and their definitions.
       |- topologies        --> stores the configuration for backend clusters that knox redirects.
                                You have to create an xml file for every backend cluster.
          |- <name of cluster1>.xml
          |- <name of cluster2>.xml
          |- ...

       |- services          --> knox is added as a service for reliability
         |- knox.service    --> main knox service
         |- knoxldapdemo.service --> [optional] knox's own demo ldap service.
                                     Only for testing. You may remove it in production.
       |- <custom cert>.jks      --> [optional] You may also provide your own certificate instead of self-signed auto generated one by this initialization action.
                                     See knox-config.yaml for details.
    ```

2.  Once the cluster has been created, Knox is configured to run on port `8443`
    on the master node in a Dataproc cluster.

    If you use the configurations and topologies in this directory as an
    example, it binds the TLS certificate to the hostname. So you should be able
    to verify the knox installation with a service like WebHDFS:

    ```bash
    # First login to the master node via ssh
    # then the below command should return 200 OK
    curl -i --cacert /usr/lib/knox/data/security/keystores/gateway-identity.pem -u guest:guest-password \
        -X GET "https://$(hostname -A | tr -d '[:space:]'):8443/gateway/example-hive-nonpii/webhdfs/v1/?op=LISTSTATUS"
    ```

3.  To learn about how to use Knox read the
    [user guide](https://knox.apache.org/books/knox-1-1-0/user-guide.html).

## Automated tests

This init action can be tested with automated script `test_knox.py`. In order to
run just use the below command from project top directory.

```bash
IMAGE_VERSION=<image_version>
bazel test knox:test_knox --test_arg=--image_version=${IMAGE_VERSION}
```

## Additional setup for Hive

To use Knox as a Hive gateway, you should configure Hive and set the transport
protocol to Http. Knox currently does not support the Hive binary transport
protocol. The below command sets the required properties:

```bash
REGION=<region>
CLUSTER_NAME=<cluster_name>
gcloud dataproc clusters create ${CLUSTER_NAME} \
    --region ${REGION}
    --initialization-actions gs://dataproc-initialization-actions-${REGION}/knox/knox.sh \
    --metadata knox-gw-config=<your knox configuration directory without gs:// prefix> \
    --properties="hive:hive.server2.thrift.http.port=10000,hive:hive.server2.thrift.http.path=cliservice,hive:hive.server2.transport.mode=http"
```

You may test access to Hive by using beeline. Beeline is already installed in
the Dataproc cluster, so you may use the master node for testing.

```bash
beeline -u "jdbc:hive2://<ADDRESS>:8443/;ssl=true;sslTrustStore=<EXPORTED_JKS>;trustStorePassword=<MASTER_SECRET>;transportMode=http;httpPath=gateway/<TOPOLOGY_NAME>/hive" -n <username> -p <password>

# For instance, if you use the default settings provided, from master node:
beeline -u "jdbc:hive2://$(hostname -A | tr -d '[:space:]'):8443/;ssl=true;sslTrustStore=/usr/lib/knox/data/security/keystores/gateway-client.jks;trustStorePassword=secret;transportMode=http;httpPath=gateway/example-hive-nonpii/hive" -n admin -p admin-password
```

## When to use Apache Knox Gateway

Apache Knox is suitable to authenticate and authorize BI tools like Tableau and
Hue that do not support Dataproc Jobs API.

An example use case is a Hive data warehouse where there are several
semi-ephemeral Dataproc clusters each accessing to different set of data on GCS.
A single node Knox installed Dataproc cluster can act as an authn/authz gateway
for all these backend Dataproc clusters. Knox gateway can be configured to use
customer's own LDAP for authentication. Authorization is done by the ACL
mechanism of Knox. Then BI tools can use any Hive ODBC/JDBC driver (e.g.,
[Cloudera's ODBC driver](https://www.cloudera.com/downloads/connectors/hive/odbc/2-6-4.html)
for Tableau) with username and password to connect each backend cluster over the
Knox gateway.
