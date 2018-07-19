# Securing Dataproc

This initialization action installs the MIT distribution of the Kerberos packages, and configure Yarn, HDFS, Hive, Spark to integrate with Kerberose, which enables in-transit data encryption.
It will also encrypt HTTP traffic (include Web UIs and shuffle) with SSL.

## Using this initialization action

You can use this initialization action to create a secured Dataproc cluster:

  Using the `gcloud` command to create a new cluster with this initialization action.

    ```bash
    gcloud dataproc clusters create <CLUSTER_NAME> \
    --scopes cloud-platform \
    --initialization-actions gs://dataproc-initialization-actions/secure/secure.sh \
    --metadata "kms-key-uri=projects/<PROJECT_ID>/locations/global/keyRings/my-key-rings/cryptoKeys/my-key" \
    --metadata "keystore-uri=gs://<SECRET-BUCKET>/keystore.jks" \
    --metadata "truststore-uri=gs://<SECRET-BUCKET>/truststore.jks" \
    --metadata "db-password-uri=gs://<SECRET-BUKCET>/dp-password.encrypted" \
    --metadata "root-password-uri=gs://<SECRET-BUCKET>/root-password.encrypted" \
    --metadata "keystore-password-uri=gs://<SECRET-BUCKET>/keystore-password.encrypted"
    ```

All the metadata key-value pairs are required to properly secure the cluster.

1. Use **keystore-uri** to specify the GCS location of the keystore file which contains the SSL certificate. It has to be in the Java KeyStore (JKS) format and when copied to VMs, renamed (if necessary) to keystore.jks. The SSL certificate should be a wildcard certificate which applies to every node in the cluster.
1. Use **truststore-uri** to specify the GCS location of the truststore file. It has to be in the Java KeyStore (JKS) format and when copied to VMs, renamed (if necessary) to truststore.jks
1. Use **keystore-password** to specify the password to the keystore and truststore files. For simplicity, the keystore password, key password, as well as the truststore password, will all be this password.
1. Use **kms-key-uri** to specify the URI of the KMS key used to encryp various password files.
1. Use **db-password-uri** to specify the GCS location of the encrypted file which contains the password to the KDC master database.
1. Use **root-password-uri** to specify the GCS location of the encrypted file which contains the password to the root user principal.

## Important notes
1. Trying to use this initialization action on a Dataproc cluster in HA mode will fail.
1. Only a root user principal "root@<REALM>" will be provided (password specfied by caller). It has administrative permission to the KDC.
1. Once the cluster is secured ("Kerberized"), submitting a Yarn job through gcloud will stop working. You can however still ssh to the master and submit a job.
