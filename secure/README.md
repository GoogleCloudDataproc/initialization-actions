# Securing Dataproc

This initialization action installs the MIT distribution of the Kerberos packages, and configures YARN, HDFS, Hive, Spark to integrate with Kerberose, which also enables in-transit data encryption.
It will also encrypt HTTP traffic (include Web UIs and shuffle) with SSL.
Optionally, this initialization action will also enable cross-realm trust, to allow users to authenticate through a remote Kerberos/Active Directory server.

## Using this initialization action
You can use this initialization action to create a secured Dataproc cluster:
  Using the `gcloud` command to create a new cluster with this initialization action.

```bash
gcloud dataproc clusters create <CLUSTER_NAME> \
    --scopes cloud-platform \
    --initialization-actions gs://dataproc-initialization-actions/secure/secure.sh \
    --metadata "kms-key-uri=projects/<PROJECT_ID>/locations/global/keyRings/my-key-rings/cryptoKeys/my-key" \
    --metadata "keystore-uri=gs://<SECRET_BUCKET>/keystore.jks" \
    --metadata "truststore-uri=gs://<SECRET_BUCKET>/truststore.jks" \
    --metadata "db-password-uri=gs://<SECRET_BUCKET>/db-password.encrypted" \
    --metadata "root-password-uri=gs://<SECRET_BUCKET>/root-password.encrypted" \
    --metadata "keystore-password-uri=gs://<SECRET_BUCKET>/keystore-password.encrypted" \
    --metadata "cross-realm-trust-realm=<REMOTE.REALM>" \
    --metadata "cross-realm-trust-kdc=<REMOTE_KDC>" \
    --metadata "cross-realm-trust-admin-server=<REMOTE_ADMIN_SERVER" \
    --metadata "cross-realm-trust-password-uri=gs://<SECRET_BUCKET>/cross-realm-trust-password.encrypted"
```

All the metadata key-value pairs are required to properly secure the cluster.

1. Use **keystore-uri** to specify the GCS location of the keystore file which contains the SSL certificate. It has to be in the Java KeyStore (JKS) format and when copied to VMs, it will be renamed (if necessary) to **keystore.jks**. The SSL certificate should be a wildcard certificate which applies to every node in the cluster.
2. Use **truststore-uri** to specify the GCS location of the truststore file. It has to be in the Java KeyStore (JKS) format and when copied to VMs, it will be renamed (if necessary) to **truststore.jks**.
3. Use **keystore-password** to specify the password to the keystore and truststore files. For simplicity, the keystore password, key password, as well as the truststore password, will all be this password.
4. Use **kms-key-uri** to specify the URI of the KMS key used to encryp various password files.
5. Use **db-password-uri** to specify the GCS location of the encrypted file which contains the password to the KDC master database.
6. Use **root-password-uri** to specify the GCS location of the encrypted file which contains the password to the root user principal.
(Below are optional for cross-realm trust)
7. Use **cross-realm-trust-realm** to specify the remote realm name.
8. Use **cross-realm-trust-kdc** to specify the hostname/address of the remote KDC server.
9. Use **cross-realm-trust-admin-server** to specify the hostname/address of the remote KDC admin server.
10. Use **cross-realm-trust-password** to specify the realm trust password.

## Protecting passwords with KMS

If you want to protect the passwords for the root principal, the KDC database, and the keystore and the truststore files, you may use [Cloud KMS](https://cloud.google.com/kms/),
Google Cloud's key management service. Proceed as follows:

1. Create a bucket to store the encrypted passwords:

    ```bash
    gsutil mb gs://<SECRET_BUCKET>
    ```

2. Create a key ring:

    ```bash
    gcloud kms keyrings create my-key-ring --location global
    ```

3. Create an encryption key:

    ```bash
    gcloud kms keys create my-key \
        --location global \
        --keyring my-key-ring \
        --purpose encryption
    ```

4. Encrypt the `root` principal password:

    ```bash
    echo "<ROOT_PASSWORD>" | \
    gcloud kms encrypt \
        --location=global  \
        --keyring=my-key-ring \
        --key=my-key \
        --plaintext-file=- \
        --ciphertext-file=root-password.encrypted
    ```

5. Encrypt the KDC database password:

    ```bash
    echo "<DB_PASSWORD>" | \
    gcloud kms encrypt \
        --location=global  \
        --keyring=my-key-ring \
        --key=my-key \
        --plaintext-file=- \
        --ciphertext-file=db-password.encrypted
    ```

6. Encrypt the keystore/truststore password:

    ```bash
    echo "<KEYSTORE_PASSWORD>" | \
    gcloud kms encrypt \
        --location=global  \
        --keyring=my-key-ring \
        --key=my-key \
        --plaintext-file=- \
        --ciphertext-file=keystore-password.encrypted
    ```
7. Upload the encrypted passwords to your secrets GCS bucket:

    ```bash
    gsutil cp root-password.encrypted db-password.encrypted keystore-password.encrypted gs://<SECRET_BUCKET>
    ```
## Generating the keystore

You will need to purchase the certificate from a Certificate Authority (CA).
Using a self-signed certificate in a production system is strongly discouraged.

1. Create a key pair (fill in the domain name, etc. on prompt.

    ```bash
    keytool -genkeypair -keystore <KEYSTORE_FILE> \
        -keyalg RSA \
        -storepass <STORE_PASS> \
        -keypass <KEY_PASS> \
        -alias <ALIAS>
    ```
2. Create a Certificate Signing Request (CSR) to the CA.

    ```bash
    keytool -certreq -keystore <KEYSTORE_FILE> \
        -alias <ALIAS> \
        -storepass <STORE_PASS> \
        -keypass <KEY_PASS> \
        -file <CSR_FILE>
    ```
3. Submit the CSR to the CA, and once you receive the reply containing the
   signed certificate (denoted as `CA_REPLY_CRT`, import the certificate to the the truststore

    ```bash
    keytool -importcert -keystore <TRUSTSTORE_FILE> \
        -alias <ALIAS> \
        -storepass <STORE_PASS> \
        -file <CA_REPLY_CRT>
    ```

## Enabling cross-realm trust

The initialization script will set up one-way trust to the remote Kerberos realm. Depending on the implementation of the remote Kerberos realm, you need to perform the following as well.
Also, you need to ensure KDC servers from both realms can communicate with each other. If your remote Kerberos server is on-prem, we recommend using [Cloud VPN](https://cloud.google.com/vpn/docs/) or [Cloud Interconnect](https://cloud.google.com/interconnect/).

### Enabling cross-realm trust to a remote MIT KDC

1. Configure the domains in krb5.conf file in your remote KDC, add the following in the `/etc/krb5.conf` file:
   ```
   [realms]
       <DATAPROC.REALM> = {
         kdc = <MASTER-NAME-OR-ADDRESS>
         admin_server = <MASTER-NAME-OR-ADDRESS>
       }
   ```
2. Create the trust user:
   ```
   kadmin -q "addprinc krbtgt/<DATAPROC.REALM>@<REMOTE.REALM>"
   ```
And when prompted, enter the password for this user. The password has to be exactly the same as the one in the encrypted file specified by the **cross-realm-trust-password-uri** parameter.

### Enabling cross-realm trust to an Active Directory

Run the following commands in a PowerShell as Adminstrator:
1. Create a KDC definition in Active Directory
   ```
   ksetup /addkdc <DATAPROC.REALM> <DATAPROC-CLUSTER-MASTER-NAME-OR-ADDRESS>
   ```
2. Create trust in Active Directory
   ```
   netdom trust <DATAPROC.REALM> /Domain <AD.REALM> /add /realm /passwordt:<TRUST-PASSWORD>
   ```
The trust password specified by **passwordt** must be the same as the one in the encrypted file specified by the **cross-realm-trust-password-uri** parameter.

## Important notes
1. This initialization action does not support Dataproc clusters in HA mode.
2. Only a root user principal "root@`<REALM>`" will be provided (password specfied by caller). It has administrative permission to the KDC.
3. Once the cluster is secured ("Kerberized"), submitting a YARN job through gcloud will stop working. You can however still ssh to the master and submit a job.
