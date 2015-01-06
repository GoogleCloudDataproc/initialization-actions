Apache Hive and Pig on Google Compute Engine
============================================

Copyright
---------

Copyright 2013 Google Inc. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.


Disclaimer
----------

This sample application is not an official Google product.


Summary
-------

This sample application can be used to install Apache Hive and/or Pig
onto a Hadoop master instance on Google Compute Engine.

Prerequisites
-------------

The sample application should be run from an Internet-connected machine
such as a workstation/laptop.

The application assumes you have a Google Cloud Project created and that
[Google Cloud Storage](https://developers.google.com/storage/docs/signup) and
[Google Compute Engine](https://developers.google.com/compute/docs/signup)
services are enabled on the project.

The application uses
[gsutil](https://developers.google.com/storage/docs/gsutil) and
[gcutil](https://developers.google.com/compute/docs/gcutil),
command line tools for Google Cloud Storage and Google Compute Engine
respectively.  Make sure to have the latest version of these
[Cloud SDK](https://developers.google.com/cloud/sdk/) tools installed
and added to your PATH environment variable.

##### Default project

The default project of the Cloud SDK tools must be set to the project
of the Hadoop cluster.

Use the [gcloud](https://developers.google.com/cloud/sdk/gcloud) command
to change the default project of the Cloud SDK:

    gcloud config set project <project ID>

##### Hadoop cluster

You should already have a Hadoop cluster running on Google Compute Engine.
This sample application has been tested with clusters created using both
of the following:

  * [Solutions Cluster for Hadoop](https://github.com/GoogleCloudPlatform/solutions-google-compute-engine-cluster-for-hadoop)
  * [Bash Script Quickstart for Hadoop](https://developers.google.com/hadoop/setting-up-a-hadoop-cluster)

In addition to a running Google Compute Engine cluster, the sample application
requires the user running the installation scripts and the Google
Compute Engine instances to have authorized access to a Google
Cloud Storage bucket.

##### Cloud Storage bucket

Create a Google Cloud Storage bucket.  This can be done by one of:

* Using an existing bucket.
  If you used either of the above Hadoop cluster bring-up packages, you may
  use the same bucket.
* Creating a new bucket from the "Cloud Storage" page on the project page of
[Developers Console](https://console.developers.google.com/)
* Creating a new bucket with the
[gsutil command line tool](https://developers.google.com/storage/docs/gsutil):

        gsutil mb gs://<bucket name>

Make sure to create the bucket in the same Google Cloud project as that
specified in the "Default project" section above.

Package Downloads
-----------------

This sample application can be used to install just one or both
of the packages discussed here.
Which packages are installed will be driven by copying the respective
tool's package archive into the "packages/_toolname_" subdirectory
of the sample app prior to running the installation scripts.

### Hive Package Setup
Create a directory for the Hive package as a subdirectory of the sample application:

    mkdir -p packages/hive

Download Hive from
[hive.apache.org](http://hive.apache.org/downloads.html)
and copy the gzipped tar file
into the `packages/hive/` subdirectory.  Testing of this sample application
was performed with `hive-0.11.0.tar.gz` and `hive-0.12.0.tar.gz`.

### Pig Package Setup
Create a directory for the Pig package as a subdirectory of the sample application::

    mkdir -p packages/pig

Download Pig from
[pig.apache.org](http://pig.apache.org/releases.html)
and copy the gzipped tar file
into the `packages/pig/` subdirectory.  Testing of this sample application
was performed with pig-0.11.1.tar.gz and pig-0.12.0.tar.gz.

If installing both tools, the packages subdirectory will
now appear as:

    packages/
      hive/
        hive-0.12.0.tar.gz
      pig/
        pig-0.12.0.tar.gz

Enter project details into properties file
------------------------------------------
Edit the file `project_properties.sh` found in the root directory of the
sample application.

Update the `GCS_PACKAGE_BUCKET` value with the bucket name of the Google
Cloud Storage associated with your Hadoop project, such as

    readonly GCS_PACKAGE_BUCKET=myproject-bucket

Update the `ZONE` value with the Compute Engine zone associated with your
Hadoop master instance, such as:

    readonly ZONE=us-central1-a

Update the `MASTER` value with the name of the Compute Engine
Hadoop master instance associated with your project, such as:

    readonly MASTER=myproject-hm

Update the `HADOOP_HOME` value with the full directory path where
hadoop is installed on the Hadoop master instance, such as:

    readonly HADOOP_HOME=/home/hadoop/hadoop

or

    readonly HADOOP_HOME=/home/hadoop/hadoop-install

The sample application will create a system userid named `hdpuser` on the
Hadoop master instance.  The software will be installed into the user's
home directory `/home/hdpuser`.

If you would like to use a different username or install the software
into a different directory, then update the `HDP_USER`, `HDP_USER_HOME`,
and `MASTER_INSTALL_DIR` values in `project_proerties.sh`.

Push packages to cloud storage
------------------------------
From the root directory where this sample application has been installed,
run:

    $ ./scripts/packages-to-gcs__at__host.sh

This command will push the packages tree structure up to the
`GCS_PACKAGE_BUCKET` configured above.

Run installation onto Hadoop master
-----------------------------------
From the root directory where this sample application has been installed,
run:

    $ ./scripts/install-packages-on-master__at__host.sh

This command will perform the installation onto the Hadoop master instance,
including the following operations:

  * Create the `hdpuser`
  * Install software packages
  * Set user privileges in the Google Compute Engine instance filesystem
  * Set user privileges in the Hadoop File System (HDFS)
  * Set up SSH keys for the `hdpuser`

Tests
-----
On successful installation, the script will emit the appropriate command
to connect to the `hdpuser` over SSH.

Once connected to the Hadoop master, the following steps will help verify
a functioning installation.

The examples here use the `/etc/passwd` file, which contains no actual
passwords and is publicly readable.

### Hive Test
On the Hadoop master instance, under the `hdpuser` copy the
`/etc/passwd` file from the file system into HDFS:

    $ hadoop fs -put /etc/passwd /tmp

Start the Hive shell with the following command:

    $ hive

At the Hive shell prompt enter:

    CREATE TABLE passwd (
      user STRING,
      dummy STRING,
      uid INT,
      gid INT,
      name STRING,
      home STRING,
      shell STRING
    )
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ':'
    STORED AS TEXTFILE;

    LOAD DATA INPATH '/tmp/passwd'
    OVERWRITE INTO TABLE passwd;

    SELECT shell, COUNT(*) shell_count
    FROM passwd
    GROUP BY shell
    ORDER BY shell_count DESC;

This should start a MapReduce job sequence to first group and sum
the shell types and secondly to sort the results.

The query results will be emitted to the console and should look
something like:

    /bin/bash 23
    /usr/sbin/nologin 21
    /bin/sync 1

To drop the table enter:

    DROP TABLE passwd;

Note that you do NOT need to remove the `passwd` file from HDFS.
The LOAD DATA command will have _moved_ the file from `/tmp/passwd`
to `/user/hdpuser/warehouse`.  Dropping the `passwd` table will
remove the file from `/user/hdpuser/warehouse`.

To exit the Hive shell enter:

    exit;

### Pig Test
On the Hadoop master instance, under the `hdpuser` copy the
`/etc/passwd` file from the file system into HDFS:

    $ hadoop fs -put /etc/passwd /tmp

Start the Pig shell with the following command:

    $ pig

At the Pig shell prompt enter:

    data = LOAD '/tmp/passwd'
           USING PigStorage(':')
           AS (user:CHARARRAY, dummy:CHARARRAY, uid:INT, gid:INT,
               name:CHARARRAY, home:CHARARRAY, shell:CHARARRAY);
    grp = GROUP data BY (shell);
    counts = FOREACH grp GENERATE
             FLATTEN(group), COUNT(data) AS shell_count:LONG;
    res = ORDER counts BY shell_count DESC;
    DUMP res;

This should start a MapReduce job sequence to first group and sum
the shell types, secondly to sample the results for the subsequent
sort job.

The query results will be emitted to the console and should look
something like:

    (/bin/bash,23)
    (/usr/sbin/nologin,21)
    (/bin/sync,1)

To exit the Pig shell enter:

    quit;

When tests are completed, the passwd file can be removed with:

    $ hadoop fs -rm /tmp/passwd


Post-installation cleanup
-------------------------
After installation, the software packages can be removed from Google Cloud Storage.

From the root directory where this sample application has been installed,
run:

    ./scripts/packages-delete-from-gcs__at__host.sh

Appendix A
----------
Using MySQL for the Hive Metastore

The default Hive installation uses a local Derby database to store Hive
meta information (table and column names, column types, etc).
This local database is fine for single-user/single-session usage, but a common
setup is to configure Hive to use a MySQL database instance.

The following sections describe how to setup Hive to use a MySQL database
either using Google Cloud SQL or a self-installed and managed MySQL database
on the Google Compute Engine cluster master instance.

### Google Cloud SQL

[Google Cloud SQL](https://developers.google.com/cloud-sql/) is a
MySQL database service on the Google Cloud Platform.  Using Cloud SQL
removes the need to install and maintain MySQL on Google Compute Engine.

The instructions here assume that you will use native MySQL JDBC driver
and not the legacy Google JDBC driver.

#### Create and configure the Google Cloud SQL instance

In the [Developers Console](https://console.developers.google.com/),
create a Google Cloud SQL instance, as described at
[Getting Started](https://developers.google.com/cloud-sql/docs/before_you_begin).
When creating the Cloud SQL instance, be sure to:

  * Select **Specify Compute Engine zone** for the **Preferred Location**
  * Select the **GCE Zone** of your Hadoop master instance
  * Select **Assign IP Address**
  * Add the external IP address of the Hadoop master instance to
    **Authorized IP Addresses**
    (you can also assign an IP address after creating the Cloud SQL instance).

After the Cloud SQL instance is created

  * Select the instance link from the Cloud SQL instance list
  * Go to the *Access Control* tab
    * Enter a root password in the appropriate field (make a note of it, you will need it for the steps below.)
    * Note the *IP address* assigned to the Cloud SQL instance;
      it will be used in the Hive configuration file as explained below.

#### Install the MySQL client

Connect to the Hadoop master instance:

    gcutil ssh <hadoop-master>

Use the `aptitude` package manager to install the MySQL client:

    sudo apt-get install --yes -f
    sudo apt-get install --yes mysql-client


#### Create the Cloud SQL database

Launch the mysql client to connect to the Google Cloud SQL instance,
replacing `<cloud-sql-addr>` with the assigned IP address of the
Cloud SQL instance

    mysql --host=<cloud-sql-addr> --user=root --password

The --password flag causes mysql to prompt for a password. Enter your root user password for the Cloud SQL instance.


Create the database `hivemeta`.  Note that Hive requires the database
to use `latin1` character encoding.

    CREATE DATABASE hivemeta CHARSET latin1;

#### Configure database user and grant privileges

Create the database user `hdpuser`:

    CREATE USER hdpuser IDENTIFIED BY 'hdppassword';

[You should select your own password here in place of _hdppassword_.]

Issue grants on the `hivemeta` database to `hdpuser`:

    GRANT ALL PRIVILEGES ON hivemeta.* TO hdpuser;

#### Install the MySQL native JDBC driver

Connect to the Hadoop master instance:

    gcutil ssh <hadoop-master>

Use the `aptitude` package manager to install the MySQL JDBC driver:

    sudo apt-get install --yes libmysql-java

#### Configure Hive to use Cloud SQL

Connect to the Hadoop master instance as the user `hdpuser`.

Add the JDBC driver JAR file to hive's CLASSPATH.
The simplest method is to copy the file to the `hive/lib/` directory:

    cp /usr/share/java/mysql-connector-java.jar hive/lib/

Update the `hive/conf/hive-site.xml` file to connect to
the Google Cloud SQL database.  Add the following configuration,
replacing `<cloud-sql-addr>` with the assigned IP address of the
Cloud SQL instance, and replacing `hdppassword` with the database
user password set earlier:

    <property>
      <name>javax.jdo.option.ConnectionURL</name>
      <value>jdbc:mysql://<cloud-sql-addr>/hivemeta?createDatabaseIfNotExist=true</value>
    </property>
    <property>
      <name>javax.jdo.option.ConnectionDriverName</name>
      <value>com.mysql.jdbc.Driver</value>
    </property>
    <property>
      <name>javax.jdo.option.ConnectionUserName</name>
      <value>hdpuser</value>
    </property>
    <property>
      <name>javax.jdo.option.ConnectionPassword</name>
      <value>hdppassword</value>
    </property>

As a password has been added to this configuration file, it is recommended that
you make the file readable and writeable only by the `hdpuser`:

    chmod 600 hive/conf/hive-site.xml

When run, Hive will now be able to use the Google Cloud SQL database as its
metastore.  The metastore database will be created with the first Hive DDL
operation.  For troubleshooting, check the Hive log at `/tmp/hdpuser/hive.log`.

### MySQL on Google Compute Engine

MySQL can be installed and run on Google Compute Engine.  The instructions
here are for installing MySQL on the Hadoop master instance.  MySQL could
also be installed on a separate Google Compute Engine instance.

#### Install the MySQL server

Connect to the Hadoop master instance:

    gcutil ssh <hadoop-master>

Use the `aptitude` package manager to install MySQL:

    sudo apt-get install --yes -f
    sudo apt-get install --yes mysql-server

#### Create MySQL database

Create a database for the Hive metastore with mysqladmin:

    sudo mysqladmin create hivemeta

When completed, create a user for the Hive metastore.

#### Configure database user and grant privileges

Launch mysql:

    sudo mysql

At the MySQL shell prompt, issue:

    CREATE USER hdpuser@localhost IDENTIFIED BY 'hdppassword';
    GRANT ALL PRIVILEGES ON hivemeta.* TO hdpuser@localhost;

[You should select your own password here in place of _hdppassword_.]

#### Install the MySQL native JDBC driver

Use the `aptitude` package manager to install the MySQL JDBC driver:

    sudo apt-get install --yes libmysql-java

#### Configure Hive to use MySQL

Connect to the Hadoop master instance as the user `hdpuser`.

Add the JDBC driver JAR file to hive's CLASSPATH.
The simplest method is to copy the file to the `hive/lib/` directory:

    cp /usr/share/java/mysql-connector-java.jar hive/lib/

Update the `hive/conf/hive-site.xml` file to connect to
the Google Cloud SQL database.  Add the following configuration,
replacing `hdppassword` with the database user password set earlier:

    <property>
      <name>javax.jdo.option.ConnectionURL</name>
      <value>jdbc:mysql://localhost/hivemeta?createDatabaseIfNotExist=true</value>
    </property>
    <property>
      <name>javax.jdo.option.ConnectionDriverName</name>
      <value>com.mysql.jdbc.Driver</value>
    </property>
    <property>
      <name>javax.jdo.option.ConnectionUserName</name>
      <value>hdpuser</value>
    </property>
    <property>
      <name>javax.jdo.option.ConnectionPassword</name>
      <value>hdppassword</value>
    </property>

As a password has been added to this configuration file, it is recommended that
you make the file readable and writeable only by the hdpuser:

    chmod 600 ~hdpuser/hive/conf/hive-site.xml

When run, Hive will now be able to use the MySQL database as its metastore.
The metastore database will be created with the first Hive DDL operation.
For troubleshooting, check the Hive log at `/tmp/hdpuser/hive.log`.

Appendix B
----------
This section lists some useful file locations on the Hadoop master
instance:

     ---------------------|---------------------------------
    | File Description    | Path                            |
    |---------------------|---------------------------------|
    | Hadoop binaries     | /home/[hadoop]/hadoop-<version> |
    | Hive binaries       | /home/[hdpuser]/hive-<version>  |
    | Pig binaries        | /home/[hdpuser]/pig-<version>   |
    | HDFS NameNode Files | /hadoop/hdfs/name               |
    | HDFS DataNode Files | /hadoop/hdfs/data               |
    | Hadoop Logs         | /var/log/hadoop                 |
    | MapReduce Logs      | /var/log/hadoop                 |
    | Hive Logs           | /tmp/[hdpuser]                  |
    | Pig Logs            | [Pig launch directory]          |
     ---------------------|---------------------------------
