# Copyright 2014 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS-IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Environment variables to be used in the local bdutil as well as in setup
# scripts running on remote VMs; this file will be used as a preamble to each
# partial setup script being run on each VM.
#
# Edit values here before running bdutil.
# CONFIGBUCKET and PROJECT are required.

############### REQUIRED ENVIRONMENT VARIABLES (no defaults) ##################

# A GCS bucket used for sharing generated SSH keys and GHFS configuration.
CONFIGBUCKET=""

# The Google Cloud Platform text-based project-id which owns the GCE resources.
PROJECT=""

###############################################################################

###################### Cluster/Hardware Configuration #########################
# These settings describe the name, location, shape and size of your cluster,
# though these settings may also be used in deployment-configuration--for
# example, to whitelist intra-cluster SSH using the cluster prefix.

# GCE settings.
GCE_IMAGE='debian-7-backports'
GCE_MACHINE_TYPE='n1-standard-4'
GCE_ZONE='us-central1-a'
# When setting a network it's important for all nodes be able to communicate
# with eachother and for SSH connections to be allowed inbound to complete
# cluster setup and configuration.
GCE_NETWORK='default'

# Prefix to be shared by all VM instance names in the cluster, as well as for
# SSH configuration between the JobTracker node and the TaskTracker nodes.
PREFIX='hadoop'

# The number of worker nodes in the cluster.
NUM_WORKERS=2

# If true, tries to attach the PDs listed in WORKER_ATTACHED_PDS and
# MASTER_ATTACHED_PD to their respective VMs as a non-boot volume. By default,
# the PDS will be named after the instance names with a "-pd" suffix.
USE_ATTACHED_PDS=false

# Only applicable if USE_ATTACHED_PDS is true; if so, this variable controls
# whether the PDs should be created explicitly during deployment. The PDs
# must not already exist.
CREATE_ATTACHED_PDS_ON_DEPLOY=true

# Only applicable if USE_ATTACHED_PDS is true; if so, this variable controls
# whether the PDs should be deleted explicitly when deleting the cluster.
DELETE_ATTACHED_PDS_ON_DELETE=true

# Only applicable during deployment if USE_ATTACHED_PDS is true and
# CREATE_ATTACHED_PDS_ON_DEPLOY is true. Specifies the size, in GB, of
# each non-boot PD to create for the worker nodes.
WORKER_ATTACHED_PDS_SIZE_GB=500

# Only applicable during deployment if USE_ATTACHED_PDS is true and
# CREATE_ATTACHED_PDS_ON_DEPLOY is true. Specifies the size, in GB, of
# the non-boot PD to create for the master node.
MASTER_ATTACHED_PD_SIZE_GB=500

# Only applicable during deployment if USE_ATTACHED_PDS is true and
# CREATE_ATTACHED_PDS_ON_DEPLOY is true. Specifies the disk type,
# either 'pd-standard' or 'pd-ssd', to create for the worker nodes.
WORKER_ATTACHED_PDS_TYPE='pd-standard'

# Only applicable during deployment if USE_ATTACHED_PDS is true and
# CREATE_ATTACHED_PDS_ON_DEPLOY is true. Specifies the disk type,
# either 'pd-standard' or 'pd-ssd', to create for the master node.
MASTER_ATTACHED_PD_TYPE='pd-standard'

# Bash array of service-account scopes to include in the created VMs.
# List of available scopes can be obtained with 'gcloud instances create --help'
# and looking under the description for "--scopes". Must at least include
# 'storage-full' for gsutil and the GCS connector to work.
GCE_SERVICE_ACCOUNT_SCOPES=('storage-full')

# List of expanded worker-node names; generally should just be derived from
# $PREFIX and $NUM_WORKERS inside 'evaluate_late_variable_bindings'; leave
# unchanged if in doubt.
WORKERS=()

# List of expanded per-worker-node PD names. Only applicable if USE_ATTACHED_PDS
# is true. Generated inside 'evaluate_late_variable_bindings' by default; leave
# unchanged if in doubt.
WORKER_ATTACHED_PDS=()

###############################################################################

#################### Deployment/Software Configuration ########################
# These settings are used by installation and configuration scripts running
# inside the VM to customize your Hadoop installation.

# Whether or not to install and configure the Cloud Storage connector.
# Must be true if DEFAULT_FS is gs
INSTALL_GCS_CONNECTOR=true

# Whether or not to install and configure the BigQuery connector.
INSTALL_BIGQUERY_CONNECTOR=false

# Whether or not to install and configure the Datastore connector.
INSTALL_DATASTORE_CONNECTOR=false

# Whether or not to configure and start HDFS
# Must be true if DEFAULT_FS is hdfs
ENABLE_HDFS=true

# Whether or not to check permissions for accessing HDFS files
ENABLE_HDFS_PERMISSIONS=false

# One of [gs|hdfs].
DEFAULT_FS='gs'

# Whether or not to enable an NFS-based cache of files and directories written
# to GCS. This helps alleviate problems with inconsistent list-operations and
# provides better support for multi-stage workflows that depend on immediate
# list-after-write consistency.
ENABLE_NFS_GCS_FILE_CACHE=true

# User to create which owns the directories used by the NFS GCS file cache
# and potentially other gcs-connector-related tasks.
GCS_ADMIN='gcsadmin'

# The user on whose behalf to run the GCS cache cleaner; not equal to GCS_ADMIN
# since the user running the cache cleaner should have "Hadoop admin"
# privileges.
GCS_CACHE_CLEANER_USER='hadoop'

# Directory in which to place logs produced by running the GCS cache cleaner;
# only applicable when ENABLE_NFS_GCS_FILE_CACHE is true. This is more
# descriptive and prescriptive; it must match the HADOOP_LOG_DIR placed inside
# ${HADOOP_CONF_DIR}/hadoop-env.sh, and it may or may not be based on the
# actual user running hadoop at runtime, depending on the Hadoop flavor being
# used.
GCS_CACHE_CLEANER_LOG_DIRECTORY='/hadoop/logs'

# The HADOOP_ROOT_LOGGER to specify when running the GCS cache cleaner; must
# be an appender already defined in Hadoop's log4j.properties file.
GCS_CACHE_CLEANER_LOGGER='INFO,DRFA'

# Options to be passed to TaskTracker child JVMs.
JAVAOPTS='-Xms1024m -Xmx2048m'

# Complete URL for downloading the GCS Connector JAR file.
GCS_CONNECTOR_JAR='https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-1.3.1-hadoop1.jar'

# Complete URL for downloading the BigQuery Connector JAR file.
BIGQUERY_CONNECTOR_JAR='https://storage.googleapis.com/hadoop-lib/bigquery/bigquery-connector-0.5.0-hadoop1.jar'

# Complete URL for downloading the Cloud Datastore Connector JAR file.
DATASTORE_CONNECTOR_JAR='https://storage.googleapis.com/hadoop-lib/datastore/datastore-connector-0.14.9-hadoop1.jar'

# Complete URL for downloading the configuration script.
BDCONFIG='https://storage.googleapis.com/hadoop-tools/bdconfig/bdconfig-0.28.1.tar.gz'

# URI of Hadoop tarball to be deployed. Must begin with gs:// or http(s)://
# Use 'gsutil ls gs://hadoop-dist/hadoop-*.tar.gz' to list Google supplied options
HADOOP_TARBALL_URI='gs://hadoop-dist/hadoop-1.2.1-bin.tar.gz'

# Directory where Hadoop is to be installed
HADOOP_INSTALL_DIR='/home/hadoop/hadoop-install'

# Directory holding config files and scripts for Hadoop
HADOOP_CONF_DIR="${HADOOP_INSTALL_DIR}/conf"

# Fraction of the master node's memory to dedicate to MapReduce
HADOOP_MASTER_MAPREDUCE_MEMORY_FRACTION=0.4

# Fraction of the master node's memory to dedicate to HDFS.
# This will be evenly split between the NameNode and Secondary NameNode.
HDFS_MASTER_MEMORY_FRACTION=0.4

# If true, strips out external apt-get mirrors from /etc/apt/sources.list
# before apt-get installing the JRE. Should only be used for
# non-critical/non-sensitive deployments due to possibly omitting security
# patches from, e.g. security.debian.org.
STRIP_EXTERNAL_MIRRORS=false

# The directory permissions to set on the datanodes' local data directories,
# used during initial configuration of HDFS as well as passed through to
# dfs.datanode.data.dir.perm in hdfs-site.xml.
HDFS_DATA_DIRS_PERM='755'

# Ports on the master node which expose useful HTTP GUIs.
# 50030 for jobtracker, 50070 for namenode.
MASTER_UI_PORTS=('50030' '50070')

###############################################################################

############################# bdutil settings #################################
# These settings don't directly affect your cluster, but simply control the
# rate, verbosity, timeouts, etc., of bdutil itself.

# Number of seconds between polling operations such as waiting for instances to
# be sshable. Should be increased for larger clusters to avoid hitting rate
# quota limits.
BDUTIL_POLL_INTERVAL_SECONDS=10

# Number of seconds, not necessarily a whole number, to sleep between
# invocations of async API calls. Mitigates flooding too many concurrent API
# calls at once during deployment.
GCLOUD_COMPUTE_SLEEP_TIME_BETWEEN_ASYNC_CALLS_SECONDS='0.1'

# If true, tee gcloud compute's stdout and stderr to console in addition to logfiles,
# otherwise only send its stdout and stderr to the logfiles.
VERBOSE_MODE=false

# If true, we will pass --verbosity=deubg to gcloud compute call sites, and -D
# to gsutil call sites, except for those occuring inside of
# validate_heavyweight_settings. Use in conjunction with VERBOSE_MODE to also
# see gcloud compute debug info on the console.
DEBUG_MODE=false

# During deployment, the maximum number of async subprocesses to use
# concurrently; can be increased if using a larger machine. Default value is
# suitable for running out of a dedicated n1-standard-1 VM.
MAX_CONCURRENT_ASYNC_PROCESSES=150

# If true, uses the old hostname convention of $PREFIX-nn and $PREFIX-dn-$i
# instead of the new $PREFIX-m and $PREFIX-w-$i. Should only be
# used if absolutely necessary for interacting with older existing clusters;
# as the old naming scheme is deprecated and will eventually be removed.
OLD_HOSTNAME_SUFFIXES=false

###############################################################################

# Helper function for normalizing boolean variables to 1/0 instead of
# true/false, respectively. We prefer to use arithmetic [1|0] instead of bash
# "true|false" and use (()) for conditions to avoid inadvertent eval of
# arbitrary strings.
function normalize_boolean() {
  local var_name=$1
  if [[ "${!var_name}" == 'true' ]]; then
    eval "${var_name}=1"
  elif [[ "${!var_name}" == 'false' ]]; then
    eval "${var_name}=0"
  fi
}

# Overridable function which will be called after sourcing all provided env
# files in sequence; allows environment variables which are derived from other
# variables to reflect overrides introduced in other files. For example, by
# computing WORKERS and MASTER_HOSTNAME as a late binding, an override file
# needs only to redefine PREFIX in order to adopt the new WORKERS and
# MASTER_HOSTNAME values as well.
function evaluate_late_variable_bindings() {
  normalize_boolean 'STRIP_EXTERNAL_MIRRORS'
  normalize_boolean 'ENABLE_HDFS'
  normalize_boolean 'INSTALL_GCS_CONNECTOR'
  normalize_boolean 'INSTALL_BIGQUERY_CONNECTOR'
  normalize_boolean 'INSTALL_DATASTORE_CONNECTOR'
  normalize_boolean 'USE_ATTACHED_PDS'
  normalize_boolean 'CREATE_ATTACHED_PDS_ON_DEPLOY'
  normalize_boolean 'DELETE_ATTACHED_PDS_ON_DELETE'
  normalize_boolean 'VERBOSE_MODE'
  normalize_boolean 'DEBUG_MODE'
  normalize_boolean 'OLD_HOSTNAME_SUFFIXES'
  normalize_boolean 'ENABLE_NFS_GCS_FILE_CACHE'

  # Generate WORKERS array based on PREFIX and NUM_WORKERS.
  local worker_suffix='w'
  local master_suffix='m'
  if (( ${OLD_HOSTNAME_SUFFIXES} )); then
    echo 'WARNING: Using deprecated -nn and -dn naming convention'
    worker_suffix='dn'
    master_suffix='nn'
  fi
  for ((i = 0; i < NUM_WORKERS; i++)); do
    WORKERS[${i}]="${PREFIX}-${worker_suffix}-${i}"
  done

  # The instance name of the VM which serves as both the namenode and
  # jobtracker.
  MASTER_HOSTNAME="${PREFIX}-${master_suffix}"

  # Generate worker PD names based on the worker instance names.
  for ((i = 0; i < NUM_WORKERS; i++)); do
    WORKER_ATTACHED_PDS[${i}]="${WORKERS[${i}]}-pd"
  done

  # List of expanded master-node PD name. Only applicable if USE_ATTACHED_PDS
  # is true.
  MASTER_ATTACHED_PD="${MASTER_HOSTNAME}-pd"

  # Fully qualified HDFS URI of namenode
  NAMENODE_URI="hdfs://${MASTER_HOSTNAME}:8020/"

  # Host and port of jobtracker
  JOB_TRACKER_URI="${MASTER_HOSTNAME}:9101"

  # GCS directory for deployment-related temporary files.
  local staging_dir_base="gs://${CONFIGBUCKET}/bdutil-staging"
  BDUTIL_GCS_STAGING_DIR="${staging_dir_base}/${MASTER_HOSTNAME}"
}

# Helper to allow env_file dependency
function import_env() {
  local env_file=$1
  if [[ -n "${BDUTIL_DIR}" ]]; then
    env_file=${BDUTIL_DIR}/${env_file}
  else
    env_file=$(basename ${env_file})
  fi
  if [[ -r ${env_file} ]]; then
    echo "Importing dependent env file: ${env_file}"
    source ${env_file}
    UPLOAD_FILES+=(${env_file})
  else
    echo "Cannot read dependent env file: ${env_file}" >&2
    exit 1
  fi
}

# Array of files, either absolute or relative to the directory where bdutil
# resides, to upload to every node before executing further commands. The files
# will all be placed in the same directory as the scripts being executed.
UPLOAD_FILES=()
if [[ -n "${BDUTIL_DIR}" ]]; then
  UPLOAD_FILES+=(${BDUTIL_DIR}/conf/hadoop1/*)
  UPLOAD_FILES+=(${BDUTIL_DIR}/libexec/hadoop_helpers.sh)
fi

# Array of strings representing mapping from command step names to the scripts
# to be executed in those steps. The first line of each group must be the name
# and end with a colon. Following the colon must be a whitespace-separated list
# of files relative to the directory where bdutil resides. Files may also be
# absolute paths.
#
# Names (portion of each element before the first ':') must be suitable for
# use as a substring inside a filename.
COMMAND_GROUPS=(
  "deploy-ssh-master-setup:
     libexec/setup_master_ssh.sh
  "

  "deploy-core-setup:
     libexec/install_java.sh
     libexec/mount_disks.sh
     libexec/setup_hadoop_user.sh
     libexec/install_hadoop.sh
     libexec/install_bdconfig.sh
     libexec/configure_hadoop.sh
     libexec/install_and_configure_gcs_connector.sh
     libexec/install_and_configure_bigquery_connector.sh
     libexec/install_and_configure_datastore_connector.sh
     libexec/configure_hdfs.sh
     libexec/set_default_fs.sh
     libexec/configure_startup_processes.sh
  "

  "deploy-master-nfs-setup:
     libexec/setup_master_nfs.sh
  "

  "deploy-client-nfs-setup:
     libexec/setup_client_nfs.sh
  "

  "deploy-ssh-worker-setup:
     libexec/setup_worker_ssh.sh
  "

  "deploy-start:
     libexec/start_hadoop.sh
  "

  # Use with run_command_group install_connectors to configure a pre-existing
  # Hadoop cluster witch the connectors.
  "install_connectors:
     libexec/install_bdconfig.sh
     libexec/install_and_configure_gcs_connector.sh
     libexec/install_and_configure_bigquery_connector.sh
     libexec/install_and_configure_datastore_connector.sh
     libexec/set_default_fs.sh
  "
)

# Array of comma-separated pairs referring to the COMMAND_GROUPS previously
# defined, of the form <invoke-on-master>,<invoke-on-all-workers>. Within
# an element, the commands will be concurrently invoked on all VMs using
# ssh sessions running in the background. All such async invocations will
# be awaited for completion before continuing to the next step.
#
# Use '*' to specify a no-op, for example if a command must be completed on
# only the master node before running the next step on all workers.
COMMAND_STEPS=(
  "deploy-ssh-master-setup,*"
  'deploy-core-setup,deploy-core-setup'
  "*,deploy-ssh-worker-setup"
  "deploy-master-nfs-setup,*"
  "deploy-client-nfs-setup,deploy-client-nfs-setup"
  "deploy-start,*"
)
