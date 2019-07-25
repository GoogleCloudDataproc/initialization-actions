# spark-nlp

This [initialization action](https://cloud.google.com/dataproc/init-actions) installs the latest version of [spark-nlp](https://github.com/JohnSnowLabs/spark-nlp)
on all nodes within a [Google Cloud Dataproc](https://cloud.google.com/dataproc) cluster.

## Using this initialization action

You can use this initialization action to create a new Cloud Dataproc cluster with spark-nlp version 2.0.8 installed. You must also include Anaconda as an [Optional Component](https://cloud.google.com/dataproc/docs/concepts/components/overview) when creating the cluster:

1. Use the `gcloud` command to create a new cluster with this initialization action.  The following command will create a new cluster named `my_cluster`:

    ```bash
    gcloud dataproc clusters create my_cluster \
      --optional-components=ANACONDA \
      --initialization-actions gs://$MY_BUCKET/spark-nlp/spark-nlp.sh
    ```
2. To use `spark-nlp` in your code, you must include `spark-nlp` with the --properties flag when submitting a job (example shows a Python job):

    ```bash
    gcloud dataproc jobs submit pyspark --cluster my-cluster \
      --properties=spark:spark.jars.packages=JohnSnowLabs:spark-nlp:2.0.8 \
      my_job.py
    ```

Note: `spark-nlp` is available for Java and Scala as well. 

## For more information

For an introduction on doing Natural Language Processing with `spark-nlp` on [Google Cloud Dataproc](https://cloud.google.com/dataproc), please check out the codelab [PySpark for Natural Language Processing](https://codelabs.developers.google.com/codelabs/spark-nlp/#0).
