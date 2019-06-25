# spark-nlp

This [initialization action](https://cloud.google.com/dataproc/init-actions) installs the latest version of [spark-nlp](https://github.com/JohnSnowLabs/spark-nlp)
on all nodes within a [Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster.

## Using this initialization action

You can use this initialization action to create a new Cloud Dataproc cluster with spark-nlp version 2.0.8 installed:

1. Use the `gcloud` command to create a new cluster with this initialization action.  The following command will create a new cluster named `my_cluster`:

    ```bash
    gcloud dataproc clusters create my_cluster \
      --initialization-actions gs://dataproc-initialization-actions/spark-nlp/spark-nlp.sh
    ```
2. To use `spark-nlp` in your code, you must include `spark-nlp` with the --properties flag when submitting a job (example shows a Python job):

    ```bash
    gcloud dataproc jobs submit pyspark --cluster my-cluster \
      --properties=spark:spark.jars.packages=JohnSnowLabs:spark-nlp:2.0.8 \
      my_job.py
    ```

Note: `spark-nlp` is available for Java and Scala as well. 
