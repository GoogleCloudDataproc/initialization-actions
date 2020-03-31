## Configuration properties

### General configuration

*   `fs.gs.project.id` (not set by default)

    Google Cloud Project ID with access to GCS buckets. Required only for list
    buckets and create bucket operations.

*   `fs.gs.working.dir` (default: `/`)

    The directory relative `gs:` uris resolve in inside of the default bucket.

*   `fs.gs.implicit.dir.repair.enable` (default: `true`)

    Whether or not to create objects for the parent directories of objects with
    `/` in their path e.g. creating `gs://bucket/foo/` upon deleting or renaming
    `gs://bucket/foo/bar`.

*   `fs.gs.copy.with.rewrite.enable` (default: `true`)

    Whether or not to perform copy operation using Rewrite requests. Allows to
    copy files between different locations and storage classes.

*   `fs.gs.rewrite.max.bytes.per.call` (default: `536870912`)

    Maximum number of bytes rewritten in a single rewrite request when
    `fs.gs.copy.with.rewrite.enable` is set to `true`.

*   `fs.gs.config.override.file` (not set by default)

    Override configuration file path. Properties defined in this file overrides
    the properties defined in `core-site.xml` and values passed from command
    line.

*   `fs.gs.reported.permissions` (default: `700`)

    Permissions that are reported for a file or directory to have regardless of
    actual Cloud Storage permissions. Can be either in octal or symbolic format
    that accepted by FsPermission class.

*   `fs.gs.delegation.token.binding` (not set by default)

    Delegation Token binding class.

*   `fs.gs.bucket.delete.enable` (default: `false`)

    If true, recursive delete on a path that refers to a Cloud Storage bucket
    itself or delete on that path when it is empty will result in deletion of
    the bucket itself. If false, any operation that normally would have deleted
    the bucket will be ignored. Setting to `false` preserves the typical
    behavior of `rm -rf /` which translates to deleting everything inside of
    root, but without clobbering the filesystem authority corresponding to that
    root path in the process.

*   `fs.gs.checksum.type` (default: `NONE`)

    Configuration of object checksum type to return; if a particular file
    doesn't support the requested type, then getFileChecksum() method will
    return `null` for that file. Supported checksum types are `NONE`, `CRC32C`
    and `MD5`

*   `fs.gs.status.parallel.enable` (default: `false`)

    If true, executes Cloud Storage object requests in FileSystem listStatus and
    getFileStatus methods in parallel to reduce latency.

*   `fs.gs.lazy.init.enable` (default: `false`)

    Enables lazy initialization of `GoogleHadoopFileSystem` instances.

*   `fs.gs.block.size` (default: `67108864`)

    The reported block size of the file system. This does not change any
    behavior of the connector or the underlying GCS objects. However it will
    affect the number of splits Hadoop MapReduce uses for a given input.

*   `fs.gs.implicit.dir.infer.enable` (default: `true`)

    Enables automatic inference of implicit directories. If set to true,
    connector creates and return in-memory directory objects on the fly when no
    backing object exists, but it could be inferred that it should exist because
    there are files with the same prefix.

*   `fs.gs.glob.flatlist.enable` (default: `true`)

    Whether or not to prepopulate potential glob matches in a single list
    request to minimize calls to GCS in nested glob cases.

*   `fs.gs.glob.concurrent.enable` (default: `true`)

    Enables concurrent execution of flat and regular glob search algorithms in
    two parallel threads to improve globbing performance. Whichever algorithm
    will finish first that result will be returned and the other algorithm
    execution one will be interrupted.

*   `fs.gs.max.requests.per.batch` (default: `15`)

    Maximum number of Cloud Storage requests that could be sent in a single
    batch request.

*   `fs.gs.batch.threads` (default: `15`)

    Maximum number of threads used to execute batch requests in parallel.

*   `fs.gs.copy.max.requests.per.batch` (default: `15`)

    Maximum number of Cloud Storage requests that could be sent in a single
    batch request for copy operations.

*   `fs.gs.copy.batch.threads` (default: `15`)

    Maximum number of threads used to execute batch requests in parallel for
    copy operations.

*   `fs.gs.list.max.items.per.call` (default: `1024`)

    Maximum number of items to return in response for list Cloud Storage
    requests.

*   `fs.gs.max.wait.for.empty.object.creation.ms` (default: `3000`)

    Maximum amount of time to wait after exception during empty object creation.

*   `fs.gs.marker.file.pattern` (not set by default)

    If set, files that match specified pattern are copied last during folder
    rename operation.

*   `fs.gs.storage.http.headers.<HEADER>=<VALUE>` (not set by default)

    Custom HTTP headers added to Cloud Storage API requests.

    Example:

    ```
    fs.gs.storage.http.headers.some-custom-header=custom_value
    fs.gs.storage.http.headers.another-custom-header=another_custom_value
    ```

### Encryption (CSEK)

*   `fs.gs.encryption.algorithm` (not set by default)

    The encryption algorithm to use. For [CSEK](https://cloud.google.com/storage/docs/encryption/customer-supplied-keys)
    Only `AES256` value is supported.

*   `fs.gs.encryption.key` (not set by default)

    An RFC 4648 Base64-encoded string of the source object's AES-256 encryption key.

*   `fs.gs.encryption.key.hash` (not set by default)

    An RFC 4648 Base64-encoded string of the SHA256 hash of the source object's encryption key.

### Authentication

When one of the following two properties is set, it will precede all other
credential settings, and credentials will be obtained from the access token
provider.

*   `fs.gs.auth.access.token.provider.impl` (not set by default)

    The implementation of the AccessTokenProvider interface used for GCS
    Connector.

*   `fs.gs.auth.service.account.enable` (default: `true`)

    Whether to use a service account for GCS authorization. If an email and
    keyfile are provided (see `fs.gs.auth.service.account.email` and
    `fs.gs.auth.service.account.keyfile`), then that service account will be
    used. Otherwise the connector will look to see if it is running on a GCE VM
    with some level of GCS access in it's service account scope, and use that
    service account.

#### Service account authentication

The following properties are required only when running not on a GCE VM and
`fs.gs.auth.service.account.enable` is `true`. There are 3 ways to configure
these credentials, which are mutually exclusive.

*   `fs.gs.auth.service.account.email` (not set by default)

    The email address is associated with the service account used for GCS access
    when `fs.gs.auth.service.account.enable` is `true`. Required when
    authentication key specified in the Configuration file (Method 1) or a
    PKCS12 certificate (Method 3) is being used.

##### Method 1

Configure service account details directly in the Configuration file or via
[Hadoop Credentials](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/CredentialProviderAPI.html).

*   `fs.gs.auth.service.account.private.key.id` (not set by default)

    The private key id associated with the service account used for GCS access.
    This can be extracted from the json keyfile generated via the Google Cloud
    Console.

*   `fs.gs.auth.service.account.private.key` (not set by default)

    The private key associated with the service account used for GCS access.
    This can be extracted from the json keyfile generated via the Google Cloud
    Console.

##### Method 2

Configure service account credentials using a json keyfile. The file must exist
at the same path on all nodes

*   `fs.gs.auth.service.account.json.keyfile` (not set by default)

    The path to the json keyfile for the service account.

##### Method 3

Configure service account credentials using a P12 certificate. The file must
exist at the same path on all nodes

*   `fs.gs.auth.service.account.keyfile` (not set by default)

    The PKCS12 (p12) certificate file of the service account used for GCS access
    when `fs.gs.auth.service.account.enable` is `true`.

#### Client secret authentication

The following properties are required when `fs.gs.auth.service.account.enable`
is `false`.

*   `fs.gs.auth.client.id` (not set by default)

    The client ID for GCS access in the OAuth 2.0 installed application flow
    (when `fs.gs.auth.service.account.enable` is `false`).

*   `fs.gs.auth.client.secret` (not set by default)

    The client secret for GCS access in the OAuth 2.0 installed application flow
    (when `fs.gs.auth.service.account.enable` is `false`).

*   `fs.gs.auth.client.file` (not set by default)

    The client credential file for GCS access in the OAuth 2.0 installed
    application flow (when `fs.gs.auth.service.account.enable` is `false`).

### IO configuration

*   `fs.gs.inputstream.buffer.size` (default: `0`)

    The number of bytes in read buffers.

*   `fs.gs.inputstream.fast.fail.on.not.found.enable` (default: `true`)

    If `true`, on opening a file connector will proactively send a Cloud Storage
    metadata request to check whether the object exists, even though the
    underlying channel will not open a data stream until `read()` method is
    called so that streams can seek to nonzero file positions without incurring
    an extra stream creation. This is necessary to technically match the
    expected behavior of HCFS, but incurs extra latency overhead on `open()`
    call. If the client code can handle late failures on not-found errors, or
    has independently already ensured that a file exists before calling open(),
    then set this property to false for more efficient reads.

*   `fs.gs.inputstream.support.gzip.encoding.enable` (default: `false`)

    If set to `false` then reading files with GZIP content encoding (HTTP header
    `Content-Encoding: gzip`) will result in failure (`IOException` is thrown).

    This feature is disabled by default because processing of
    [GZIP encoded](https://cloud.google.com/storage/docs/transcoding#decompressive_transcoding)
    files is inefficient and error-prone in Hadoop and Spark.

*   `fs.gs.outputstream.buffer.size` (default: `8388608`)

    Write buffer size.

*   `fs.gs.outputstream.pipe.buffer.size` (default: `1048576`)

    Pipe buffer size used for uploading Cloud Storage objects.

*   `fs.gs.outputstream.upload.chunk.size` (default: `67108864`)

    The number of bytes in one GCS upload request.

*   `fs.gs.outputstream.upload.cache.size` (default: `0`)

    The upload cache size in bytes used for high-level upload retries. To
    disable this feature set this property to zero or negative value. Retry will
    be performed if total size of written/uploaded data to the object is less
    than or equal to the cache size. 

*   `fs.gs.outputstream.direct.upload.enable` (default: `false`)

    Enables Cloud Storage direct uploads.

*   `fs.gs.outputstream.type` (default: `BASIC`)

    Output stream type to use; different options may have different degrees of
    support for advanced features like `hsync()` and different performance
    characteristics.

    Valid options:

    *   `BASIC` - stream is closest analogue to direct wrapper around low-level
        HTTP stream into GCS.

    *   `SYNCABLE_COMPOSITE` - stream behaves similarly to `BASIC` when used
        with basic create/write/close patterns, but supports `hsync()` by
        creating discrete temporary GCS objects which are composed onto the
        destination object.

### HTTP transport configuration

*   `fs.gs.http.transport.type` (default: `JAVA_NET`)

    HTTP transport to use for sending Cloud Storage requests. Valid values are
    `APACHE` or `JAVA_NET`.

*   `fs.gs.application.name.suffix` (not set by default)

    Suffix that will be added to HTTP `User-Agent` header set in all Cloud
    Storage requests.

*   `fs.gs.proxy.address` (not set by default)

    Proxy address that connector can use to send Cloud Storage requests. The
    proxy must be an HTTP proxy and address should be in the `host:port` form.

*   `fs.gs.proxy.username` (not set by default)

    Proxy username that connector can use to send Cloud Storage requests.

*   `fs.gs.proxy.password` (not set by default)

    Proxy password that connector can use to send Cloud Storage requests.

*   `fs.gs.http.max.retry` (default: `10`)

    The maximum number of retries for low-level HTTP requests to GCS when server
    errors (code: `5XX`) or I/O errors are encountered.

*   `fs.gs.http.connect-timeout` (default: `20000`)

    Timeout in milliseconds to establish a connection. Use `0` for an infinite
    timeout.

*   `fs.gs.http.read-timeout` (default: `20000`)

    Timeout in milliseconds to read from an established connection. Use `0` for
    an infinite timeout.

### API client configuration

*   `fs.gs.storage.root.url`

    Google Cloud Storage root URL.

*   `fs.gs.token.server.url`

    Google Token Server root URL.

### Fadvise feature configuration

*   `fs.gs.inputstream.fadvise` (default: `AUTO`)

    Tunes reading objects behavior to optimize HTTP GET requests for various use
    cases.

    This property controls fadvise feature that allows to read objects in
    different modes:

    *   `SEQUENTIAL` - in this mode connector sends a single streaming
        (unbounded) Cloud Storage request to read object from a specified
        position sequentially.

    *   `RANDOM` - in this mode connector will send bounded Cloud Storage range
        requests (specified through HTTP Range header) which are more efficient
        in some cases (e.g. reading objects in row-columnar file formats like
        ORC, Parquet, etc).

        Range request size is limited by whatever is greater, `fs.gs.io.buffer`
        or read buffer size passed by a client.

        To avoid sending too small range requests (couple bytes) - could happen
        if `fs.gs.io.buffer` is 0 and client passes very small read buffer,
        minimum range request size is limited to 1 MiB by default configurable
        through `fs.gs.inputstream.min.range.request.size` property

    *   `AUTO` - in this mode (adaptive range reads) connector starts to send
        bounded range requests when reading non gzip-encoded objects instead of
        streaming requests as soon as first backward read or forward read for
        more than `fs.gs.inputstream.inplace.seek.limit` bytes was detected.

*   `fs.gs.inputstream.inplace.seek.limit` (default: `8388608`)

    If forward seeks are within this many bytes of the current position, seeks
    are performed by reading and discarding bytes in-place rather than opening a
    new underlying stream.

*   `fs.gs.inputstream.min.range.request.size` (default: `524288`)

    Minimum size in bytes of the read range for Cloud Storage request when
    opening a new stream to read an object.

### Performance cache configuration

*   `fs.gs.performance.cache.enable` (default: `false`)

    Enables a performance cache that temporarily stores successfully queried
    Cloud Storage objects in memory. Caching provides a faster access to the
    recently queried objects, but because objects metadata is cached,
    modifications made outside of this connector instance may not be immediately
    reflected.

*   `fs.gs.performance.cache.max.entry.age.ms` (default: `5000`)

    Maximum number of milliseconds to store a cached metadata in the performance
    cache before it's invalidated.

### Cloud Storage [Requester Pays](https://cloud.google.com/storage/docs/requester-pays) feature configuration:

*   `fs.gs.requester.pays.mode` (default: `DISABLED`)

    Valid values:

    *   `AUTO` - Requester Pays feature enabled only for GCS buckets that
        require it;

    *   `CUSTOM` - Requester Pays feature enabled only for GCS buckets that are
        specified in the `fs.gs.requester.pays.buckets`;

    *   `DISABLED` - Requester Pays feature disabled for all GCS buckets;

    *   `ENABLED` - Requester Pays feature enabled for all GCS buckets.

*   `fs.gs.requester.pays.project.id` (not set by default)

    Google Cloud Project ID that will be used for billing when GCS Requester
    Pays feature is active (in `AUTO`, `CUSTOM` or `ENABLED` mode). If not
    specified and GCS Requester Pays is active then value of the
    `fs.gs.project.id` property will be used.

*   `fs.gs.requester.pays.buckets` (not set by default)

    Comma-separated list of Google Cloud Storage Buckets for which GCS Requester
    Pays feature should be activated if `fs.gs.requester.pays.mode` property
    value is set to `CUSTOM`.

### Cooperative Locking feature configuration

*   `fs.gs.cooperative.locking.enable` (default: `false`)

    Enables cooperative locking to achieve isolation of directory mutation
    operations.

*   `fs.gs.cooperative.locking.expiration.timeout.ms` (default: `120000`)

    Lock expiration timeout used by cooperative locking feature to lock
    directories.

*   `fs.gs.cooperative.locking.max.concurrent.operations` (default: `20`)

    Maximum number of concurrent directory modification operations per bucket
    guarded by cooperative locking feature.
