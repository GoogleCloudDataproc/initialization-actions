# Apache Datasketches

**:warning: NOTICE:** This init action is supported only on Dataproc clusters 2.1 and above.

This initialization action installs libraries required to run [Apache Datasketches](https://datasketches.apache.org/) on a
[Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster.

## Using this initialization action

**:warning: NOTICE:** See
[best practices](/README.md#how-initialization-actions-are-used) of using
initialization actions in production.

This initialization action installs dataksketches libraries on Dataproc cluster at `/usr/lib/datasketches` location, below jars will be deployed:

```
datasketches-memory-2.0.0.jar
datasketches-java-3.1.0.jar
datasketches-pig-1.1.0.jar
datasketches-hive-1.2.0.jar
spark-java-thetasketches-1.0-SNAPSHOT.jar [ Only if Spark version < 3.5.0 ]
```

1.  Using the `gcloud` command to create a new cluster with this initialization
    action. The following command will create a new standard cluster named
    `${CLUSTER_NAME}`.

    ```bash
    REGION=<region>
    CLUSTER_NAME=<cluster_name>
    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region ${REGION} \
        --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/datasketches/dataksketches.sh
    ```

## Apache Datasketches Examples:

### Spark:

Note: Starting Apache Spark version 3.5.0, Datasketches libraries are already integrated, follow this [example](https://www.databricks.com/blog/apache-spark-3-apache-datasketches-new-sketch-based-approximate-distinct-counting)

1. For Older 3.X Spark versions, follow [Thetasketches example](https://datasketches.apache.org/docs/Theta/ThetaSparkExample.html) from Datasketches documentation.  

   Note: `spark-java-thetasketches` example jar will be available under `/usr/lib/datasketches` as a part of this init action, run `spark-submit` with `spark-java-thetasketches-1.0-SNAPSHOT.jar` to try Thetasketches example. 

    ```
   spark-submit --jars /usr/lib/datasketches/datasketches-java-3.1.0.jar,/usr/lib/datasketches/datasketches-memory-2.0.0.jar --class Aggregate target/spark-java-thetasketches-1.0-SNAPSHOT.jar
   ```

   If you modify the [java code](https://datasketches.apache.org/docs/Theta/ThetaSparkExample.html), use below instructions to build the jar.

   1. Generate artifacts with Maven:

      ```
      mvn archetype:generate -DgroupId=org.apache.datasketches -DartifactId=spark-java-thetasketches -DarchetypeArtifactId=maven-archetype-quickstart -DinteractiveMode=false
      ```

   1. Replace pom.xml with https://github.com/GoogleCloudDataproc/initialization-actions/tree/master/datasketches/pom.xml


   1. Add modified code from https://datasketches.apache.org/docs/Theta/ThetaSparkExample.html under $local_path/src/main/java/org/apache/datasketches directory, remove the sample App.java file 

      Example:

      ```
      root@cluster-$hostname-m:$local_path/spark-java-thetasketches/src/main/java/org/apache/datasketches# ls -lrt
      total 20
      -rw-r--r-- 1 root root 1920 Feb 21 17:03 ThetaSketchJavaSerializable.java
      -rw-r--r-- 1 root root 2459 Feb 21 17:03 Spark2DatasetMapPartitionsReduceJavaSerialization.java
      -rw-r--r-- 1 root root 3654 Feb 21 17:03 MapPartitionsToPairReduceByKey.java
      -rw-r--r-- 1 root root 3142 Feb 21 17:03 AggregateByKey2.java
      -rw-r--r-- 1 root root 2123 Feb 21 17:03 Aggregate.java
      ```

   1. Compile the code and package a jar:

      ```
      mvn package
      ```

   1. Verify if jar is created under `target/`

      ```
      root@cluster-$hostname-m:$local_path/spark-java-thetasketches# ls -lrt target/
      total 48
      drwxr-xr-x 3 root root  4096 Feb 29 18:36 maven-status
      drwxr-xr-x 3 root root  4096 Feb 29 18:36 generated-sources
      drwxr-xr-x 2 root root  4096 Feb 29 18:36 classes
      drwxr-xr-x 3 root root  4096 Feb 29 18:36 generated-test-sources
      drwxr-xr-x 3 root root  4096 Feb 29 18:36 test-classes
      drwxr-xr-x 2 root root  4096 Feb 29 18:36 surefire-reports
      drwxr-xr-x 2 root root  4096 Feb 29 18:36 maven-archiver
      -rw-r--r-- 1 root root 17542 Feb 29 18:36 spark-java-thetasketches-1.0-SNAPSHOT.jar
      ```

   1. Run `spark-submit` with newly generated jar from above step.

      ```
      root@cluster-$hostname-m:$local_path/spark-java-thetasketches# spark-submit --jars /usr/lib/datasketches/datasketches-java-3.1.0.jar,/usr/lib/datasketches/datasketches-memory-2.0.0.jar --class Aggregate target/spark-java-thetasketches-1.0-SNAPSHOT.jar
      ```

### Hive:

1.  cd to `/usr/lib/datasketches` and follow [Datasketches Hive examples](https://datasketches.apache.org/docs/SystemIntegrations/ApacheHiveIntegration.html)

#### Pig:

1. cd to `/usr/lib/datasketches` and follow [Datasketches Pig examples](https://datasketches.apache.org/docs/SystemIntegrations/ApachePigIntegration.html)

