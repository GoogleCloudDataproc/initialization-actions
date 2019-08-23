### Introducing Cloud Storage Connector Cooperative Locking

We are excited to release Cooperative Locking support for Cloud Storage
Connector 2.0. With this release you will be able to isolate and recover
directory modification operations performed through `hadoop fs` command line
interface on Cloud Storage.

The key motivation behind this feature is a fact that directory mutations are
not atomic on Cloud Storage, which means that competing directory mutations are
susceptible to race conditions that can lead to data integrity issues (loss,
duplication, etc).

### How it works

Cooperative Locking is implemented in Cloud Storage Connector by acquiring
exclusive time-expiring lock for modified directories using Cloud Storage
[preconditions](https://cloud.google.com/storage/docs/generations-preconditions#_Preconditions)
in the `gs://<bucket>/_lock/all.lock` file. This lock is acquired before each
directory modification operation in the bucket and after that it periodically
renewed during operation execution until operation will finish.

Cooperative Locking works only for operations performed on directories in the
same bucket and as a result directory modification operations of the single
directory tree in the bucket are sequential.

All operations with expired locks could be recovered with FSCK tool.

### Key benefits

*   Write isolation of move and delete directory operations performed on Cloud
    Storage through `hadoop fs` command line interface. This prevents
    inconsistency when performing concurrent directory modification operations
    on the same directory tree.
*   Logging of objects modified by directory modification operations.
*   Recovery of failed directory modification operations using FSCK command line
    tool. Such operations can fail because of client JVM failure, network
    connection failure, etc.

### Enabling Cooperative Locking

To enable this feature set `fs.gs.cooperative.locking.enable` Hadoop property to
true in `core-site.xml`:

```xml
<configuration>
  <property>
    <name>fs.gs.cooperative.locking.enable</name>
    <value>true</value>
  </property>
</configuration>
```

or specify it in directly `hadoop fs` command:

```shell
hadoop fs -Dfs.gs.cooperative.locking.enable=true ...
```

### Using Cooperative Locking

To perform isolated directory modification operations inside the same bucket you
should use `hadoop fs` command with enabled Cooperative Locking:

```shell
hadoop fs -{mv|rm} gs://bucket/path/to/dir1 [gs://bucket/path/to/dir2]
```

### Using FSCK tool

To recover failed directory modification performed with enabled Cooperative
Locking you should use FSCK tool:

```shell
hadoop jar /usr/lib/hadoop/lib/gcs-connector.jar \
  com.google.cloud.hadoop.fs.gcs.CoopLockFsck \
  --{check|rollBack|rollForward} gs://<bucket_name> [all|<operation-id>]
```

This command will recover (roll back or roll forward) all failed directory
modification operations based on operation log.

### Configuration

See
[Cooperative Locking](CONFIGURATION.md#cooperative-locking-feature-configuration)
section in [CONFIGURATION.md](CONFIGURATION.md).

### Limitations

Because Cooperative Locking feature is intended to be used by human operators
when modifying Cloud Storage through `hadoop fs` interface, it has multiple
limitations that you should take into account before using this feature:

*   It supports isolation of directory modification operations only in the same
    bucket.
*   It has a configurable limit (20 by default) of simultaneous directory
    modification operations per-bucket.
*   It requires write and list access to the bucket for users that use
    Cooperative Locking to modify directories in the bucket.
*   It supports only `hadoop fs` tool and does not support any other Cloud
    Storage clients (gsutil, API libraries, etc).
*   To work as intended, if cooperative locking is enabled on one of the clients
    it also should be enabled on all other clients that modify the same bucket.

### Troubleshooting

To troubleshoot Cooperative Locking feature you can use extensive debug logs
that can be enabled with `--logglevel` flag:

```shell
hadoop --loglevel debug fs -Dfs.gs.cooperative.locking.enable=true -{mv|rm} \
  gs://bucket/path/to/dir1 [gs://gs://bucket/path/to/dir2]
hadoop --loglevel debug jar /usr/lib/hadoop/lib/gcs-connector.jar \
  com.google.cloud.hadoop.fs.gcs.CoopLockFsck \
  --{check|rollBack|rollForward} gs://<bucket_name> [all|<operation-id>]
```

Also you may want to inspect `all.lock`, `*.lock` and `*.log` files for specific
directory operations in `_lock/` folder:

*   `all.lock` file

    ```shell
    gsutil ls -L gs://example-bucket/_lock/all.lock | \
      grep "lock:" | awk '{print $2}' | base64 --decode | jq
    ```

    Example of the `all.lock` file `lock` metadata value that has a record of an
    active lock for a rename operation with operation client host name that
    performs this operation, unique operation ID, operation start time,
    operation type initial (not renewed) lock expiration time and locked source
    and destination directories:

    ```json
    {
      "formatVersion": 3,
      "locks": [
        {
          "clientId": "your-host.example.com-232396",
          "operationId": "d265a5f9-bce0-4ccc-b1a7-9e68476f1db8",
          "operationTime": "2019-08-22T16:26:36.339985Z",
          "operationType": "RENAME",
          "lockExpiration": "2019-08-22T16:29:12.396382Z",
          "resources": [
            "rename_dst_dir/",
            "rename_src_dir/"
          ]
        }
      ]
    }
    ```

*   Operation `*.lock` file

    ```shell
    gsutil cat gs://example-bucket/_lock/20190820T163409.446Z_RENAME_2f6e9ca4-0406-4612-82d5-7f07de81aeb0.lock | jq
    ```

    Example of a rename operation `*.lock` file content with operation lock
    expiration, source and destination directories and `copySucceeded` flag that
    indicates if copy stage of directory rename operation successfully
    completed:

    ```json
    {
      "lockExpiration": "2019-08-22T16:31:22.396382Z",
      "srcResource": "gs://example-bucket/rename_src_dir/",
      "dstResource": "gs://example-bucket/rename_dst_dir/",
      "copySucceeded": false
    }
    ```

*   Operation `*.log` file

    ```shell
    gsutil cat gs://example-bucket/_lock/20190820T163409.446Z_RENAME_2f6e9ca4-0406-4612-82d5-7f07de81aeb0.log | jq
    ```

    Example of a rename operation `*.log` file content with a each record
    representing individual file move with source and destination paths:

    ```json
    {
      "src": "gs://example-bucket/rename_src_dir/f_1",
      "dst": "gs://example-bucket/rename_dst_dir/f_1"
    }
    {
      "src": "gs://example-bucket/rename_src_dir/f_2",
      "dst": "gs://example-bucket/rename_dst_dir/f_2"
    }
    {
      "src": "gs://example-bucket/rename_src_dir/f_3",
      "dst": "gs://example-bucket/rename_dst_dir/f_3"
    }
    ```
