#!/bin/bash

set -euxo pipefail

function get_metadata_attribute() {
  local -r attribute_name=$1
  local -r default_value=$2
  /usr/share/google/get_metadata_value "attributes/${attribute_name}" || echo -n "${default_value}"
}

OS_NAME=$(lsb_release -is | tr '[:upper:]' '[:lower:]')
readonly OS_NAME

readonly SPARK_VERSION_ENV=$(spark-submit --version 2>&1 | sed -n 's/.*version[[:blank:]]\+\([0-9]\+\.[0-9]\).*/\1/p' | head -n1)
readonly DEFAULT_SPARK_RAPIDS_VERSION="23.02.0"

if [[ "${SPARK_VERSION_ENV}" == "3"* ]]; then
  readonly DEFAULT_CUDA_VERSION="11.5"
  readonly DEFAULT_XGBOOST_VERSION="1.7.1"
  readonly SPARK_VERSION="3.0"
else
  readonly DEFAULT_CUDA_VERSION="10.1"
  readonly DEFAULT_XGBOOST_VERSION="1.0.0"
  readonly DEFAULT_XGBOOST_GPU_SUB_VERSION="Beta5"
  readonly SPARK_VERSION="2.x"
  readonly XGBOOST_GPU_SUB_VERSION=$(get_metadata_attribute 'spark-gpu-sub-version' ${DEFAULT_XGBOOST_GPU_SUB_VERSION})
fi

readonly ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
readonly MASTER=$(/usr/share/google/get_metadata_value attributes/dataproc-master)

readonly RUNTIME=$(get_metadata_attribute 'rapids-runtime' 'SPARK')

# CUDA version and Driver version config
CUDA_VERSION=$(get_metadata_attribute 'cuda-version' '11.5')
DEFAULT_NVIDIA_DEBIAN_GPU_DRIVER_VERSION=$(get_metadata_attribute 'driver-version' '495.29.05')

readonly DEFAULT_NVIDIA_DEBIAN_GPU_DRIVER_VERSION
readonly DEFAULT_NVIDIA_DEBIAN_GPU_DRIVER_VERSION_PREFIX=${DEFAULT_NVIDIA_DEBIAN_GPU_DRIVER_VERSION%%.*}

# Parameters for NVIDIA-provided Debian GPU driver
readonly DEFAULT_NVIDIA_DEBIAN_GPU_DRIVER_URL="https://download.nvidia.com/XFree86/Linux-x86_64/${DEFAULT_NVIDIA_DEBIAN_GPU_DRIVER_VERSION}/NVIDIA-Linux-x86_64-${DEFAULT_NVIDIA_DEBIAN_GPU_DRIVER_VERSION}.run"
NVIDIA_DEBIAN_GPU_DRIVER_URL=$(get_metadata_attribute 'gpu-driver-url' "${DEFAULT_NVIDIA_DEBIAN_GPU_DRIVER_URL}")
readonly NVIDIA_DEBIAN_GPU_DRIVER_URL

readonly NVIDIA_BASE_DL_URL='https://developer.download.nvidia.com/compute'

# Parameters for NVIDIA-provided Debian GPU driver
readonly -A DEFAULT_NVIDIA_DEBIAN_CUDA_URLS=(
  [10.1]="${NVIDIA_BASE_DL_URL}/cuda/10.1/Prod/local_installers/cuda_10.1.243_418.87.00_linux.run"
  [10.2]="${NVIDIA_BASE_DL_URL}/cuda/10.2/Prod/local_installers/cuda_10.2.89_440.33.01_linux.run"
  [11.0]="${NVIDIA_BASE_DL_URL}/cuda/11.0.3/local_installers/cuda_11.0.3_450.51.06_linux.run"
  [11.1]="${NVIDIA_BASE_DL_URL}/cuda/11.1.0/local_installers/cuda_11.1.0_455.23.05_linux.run"
  [11.2]="${NVIDIA_BASE_DL_URL}/cuda/11.2.2/local_installers/cuda_11.2.2_460.32.03_linux.run"
  [11.5]="${NVIDIA_BASE_DL_URL}/cuda/11.5.2/local_installers/cuda_11.5.2_495.29.05_linux.run"
  [11.6]="${NVIDIA_BASE_DL_URL}/cuda/11.6.2/local_installers/cuda_11.6.2_510.47.03_linux.run"
  [11.7]="${NVIDIA_BASE_DL_URL}/cuda/11.7.1/local_installers/cuda_11.7.1_515.65.01_linux.run"
  [11.8]="${NVIDIA_BASE_DL_URL}/cuda/11.8.0/local_installers/cuda_11.8.0_520.61.05_linux.run")
readonly DEFAULT_NVIDIA_DEBIAN_CUDA_URL=${DEFAULT_NVIDIA_DEBIAN_CUDA_URLS["${CUDA_VERSION}"]}
NVIDIA_DEBIAN_CUDA_URL=$(get_metadata_attribute 'cuda-url' "${DEFAULT_NVIDIA_DEBIAN_CUDA_URL}")
readonly NVIDIA_DEBIAN_CUDA_URL

# Parameters for NVIDIA-provided Ubuntu GPU driver
NVIDIA_UBUNTU_REPO_URL="${NVIDIA_BASE_DL_URL}/cuda/repos/ubuntu1804/x86_64"
NVIDIA_UBUNTU_REPO_CUDA_PIN="${NVIDIA_UBUNTU_REPO_URL}/cuda-ubuntu1804.pin"
readonly NVIDIA_UBUNTU_REPO_KEY_PACKAGE="${NVIDIA_UBUNTU_REPO_URL}/cuda-keyring_1.0-1_all.deb"

SECURE_BOOT="disabled"
SECURE_BOOT=$(mokutil --sb-state|awk '{print $2}')

if [[ ${DATAPROC_IMAGE_VERSION} == 2.1 ]]; then
  NVIDIA_UBUNTU_REPO_URL="${NVIDIA_BASE_DL_URL}/cuda/repos/ubuntu2004/x86_64"
  NVIDIA_UBUNTU_REPO_CUDA_PIN="${NVIDIA_UBUNTU_REPO_URL}/cuda-ubuntu2004.pin"
fi

# Parameter for NVIDIA-provided Rocky Linux GPU driver
readonly NVIDIA_ROCKY_REPO_URL="${NVIDIA_BASE_DL_URL}/cuda/repos/rhel8/x86_64/cuda-rhel8.repo"

# Whether to install NVIDIA-provided or OS-provided GPU driver
GPU_DRIVER_PROVIDER=$(get_metadata_attribute 'gpu-driver-provider' 'NVIDIA')
readonly GPU_DRIVER_PROVIDER

# Stackdriver GPU agent parameters
# Whether to install GPU monitoring agent that sends GPU metrics to Stackdriver
INSTALL_GPU_AGENT=$(get_metadata_attribute 'install-gpu-agent' 'false')
readonly INSTALL_GPU_AGENT

# Dataproc configurations
readonly HADOOP_CONF_DIR='/etc/hadoop/conf'
readonly HIVE_CONF_DIR='/etc/hive/conf'
readonly SPARK_CONF_DIR='/etc/spark/conf'

NVIDIA_SMI_PATH='/usr/bin'
MIG_MAJOR_CAPS=0
IS_MIG_ENABLED=0

# SPARK config
readonly SPARK_RAPIDS_VERSION=$(get_metadata_attribute 'spark-rapids-version' ${DEFAULT_SPARK_RAPIDS_VERSION})
readonly XGBOOST_VERSION=$(get_metadata_attribute 'xgboost-version' ${DEFAULT_XGBOOST_VERSION})

function execute_with_retries() {
  local -r cmd=$1
  for ((i = 0; i < 10; i++)); do
    if eval "$cmd"; then
      return 0
    fi
    sleep 5
  done
  return 1
}

function install_spark_rapids() {
  local -r rapids_repo_url='https://repo1.maven.org/maven2/ai/rapids'
  local -r nvidia_repo_url='https://repo1.maven.org/maven2/com/nvidia'
  local -r dmlc_repo_url='https://repo.maven.apache.org/maven2/ml/dmlc'

  if [[ "${SPARK_VERSION}" == "3"* ]]; then
    wget -nv --timeout=30 --tries=5 --retry-connrefused \
      "${dmlc_repo_url}/xgboost4j-spark-gpu_2.12/${XGBOOST_VERSION}/xgboost4j-spark-gpu_2.12-${XGBOOST_VERSION}.jar" \
      -P /usr/lib/spark/jars/
    wget -nv --timeout=30 --tries=5 --retry-connrefused \
      "${dmlc_repo_url}/xgboost4j-gpu_2.12/${XGBOOST_VERSION}/xgboost4j-gpu_2.12-${XGBOOST_VERSION}.jar" \
      -P /usr/lib/spark/jars/
    wget -nv --timeout=30 --tries=5 --retry-connrefused \
      "${nvidia_repo_url}/rapids-4-spark_2.12/${SPARK_RAPIDS_VERSION}/rapids-4-spark_2.12-${SPARK_RAPIDS_VERSION}.jar" \
      -P /usr/lib/spark/jars/
  else
    wget -nv --timeout=30 --tries=5 --retry-connrefused \
      "${rapids_repo_url}/xgboost4j-spark_${SPARK_VERSION}/${XGBOOST_VERSION}-${XGBOOST_GPU_SUB_VERSION}/xgboost4j-spark_${SPARK_VERSION}-${XGBOOST_VERSION}-${XGBOOST_GPU_SUB_VERSION}.jar" \
      -P /usr/lib/spark/jars/
    wget -nv --timeout=30 --tries=5 --retry-connrefused \
      "${rapids_repo_url}/xgboost4j_${SPARK_VERSION}/${XGBOOST_VERSION}-${XGBOOST_GPU_SUB_VERSION}/xgboost4j_${SPARK_VERSION}-${XGBOOST_VERSION}-${XGBOOST_GPU_SUB_VERSION}.jar" \
      -P /usr/lib/spark/jars/
  fi
}

function configure_spark() {
  if [[ "${SPARK_VERSION}" == "3"* ]]; then
    cat >>${SPARK_CONF_DIR}/spark-defaults.conf <<EOF

###### BEGIN : RAPIDS properties for Spark ${SPARK_VERSION} ######
# Rapids Accelerator for Spark can utilize AQE, but when the plan is not finalized,
# query explain output won't show GPU operator, if user have doubt
# they can uncomment the line before seeing the GPU plan explain, but AQE on gives user the best performance.
spark.executor.resource.gpu.amount=1
spark.plugins=com.nvidia.spark.SQLPlugin
spark.executor.resource.gpu.discoveryScript=/usr/lib/spark/scripts/gpu/getGpusResources.sh
spark.dynamicAllocation.enabled=false
spark.sql.autoBroadcastJoinThreshold=10m
spark.sql.files.maxPartitionBytes=512m
# please update this config according to your application
spark.task.resource.gpu.amount=0.25
###### END   : RAPIDS properties for Spark ${SPARK_VERSION} ######
EOF
  else
    cat >>${SPARK_CONF_DIR}/spark-defaults.conf <<EOF

###### BEGIN : RAPIDS properties for Spark ${SPARK_VERSION} ######
spark.submit.pyFiles=/usr/lib/spark/jars/xgboost4j-spark_${SPARK_VERSION}-${XGBOOST_VERSION}-${XGBOOST_GPU_SUB_VERSION}.jar
###### END   : RAPIDS properties for Spark ${SPARK_VERSION} ######
EOF
  fi
}

# Install NVIDIA GPU driver provided by NVIDIA
function install_nvidia_gpu_driver() {
  if [[ ${OS_NAME} == debian ]]; then
    curl -fsSL --retry-connrefused --retry 3 --retry-max-time 5 \
      "${NVIDIA_UBUNTU_REPO_KEY_PACKAGE}" -o /tmp/cuda-keyring.deb
    dpkg -i "/tmp/cuda-keyring.deb"

    curl -fsSL --retry-connrefused --retry 3 --retry-max-time 5 \
      "${NVIDIA_DEBIAN_GPU_DRIVER_URL}" -o driver.run
    bash "./driver.run" --silent --install-libglvnd

    curl -fsSL --retry-connrefused --retry 3 --retry-max-time 5 \
      "${NVIDIA_DEBIAN_CUDA_URL}" -o cuda.run
    bash "./cuda.run" --silent --toolkit --no-opengl-libs
  elif [[ ${OS_NAME} == ubuntu ]]; then
    
    # we need to install additional modules with enabling secure boot, see issue: https://github.com/GoogleCloudDataproc/initialization-actions/issues/1043
    # following [guide](https://cloud.google.com/compute/docs/gpus/install-drivers-gpu#secure-boot) for detailed information.
    if [[ ${SECURE_BOOT} == enabled ]]; then
      NVIDIA_DRIVER_VERSION=$(apt-cache search 'linux-modules-nvidia-[0-9]+-gcp$' | awk '{print $1}' | sort | tail -n 1 | head -n 1 | awk -F"-" '{print $4}')
      apt install linux-modules-nvidia-${NVIDIA_DRIVER_VERSION}-gcp -y
      apt install nvidia-driver-${NVIDIA_DRIVER_VERSION} -y
      
      echo """
        Package: nsight-compute
        Pin: origin *ubuntu.com*
        Pin-Priority: -1
        
        Package: nsight-systems
        Pin: origin *ubuntu.com*
        Pin-Priority: -1
        
        Package: nvidia-modprobe
        Pin: release l=NVIDIA CUDA
        Pin-Priority: 600
        
        Package: nvidia-settings
        Pin: release l=NVIDIA CUDA
        Pin-Priority: 600
        
        Package: *
        Pin: release l=NVIDIA CUDA
        Pin-Priority: 100
        """ > /etc/apt/preferences.d/cuda-repository-pin-600
        
      apt install software-properties-common -y
      
      apt-key adv --fetch-keys ${NVIDIA_UBUNTU_REPO_URL}/3bf863cc.pub
      add-apt-repository "deb ${NVIDIA_UBUNTU_REPO_URL} /"
      
      # CUDA_DRIVER_VERSION should be like "525.60.13-1"
      CUDA_DRIVER_VERSION=$(apt-cache madison cuda-drivers | awk '{print $3}' | sort -r | while read line; do
        if dpkg --compare-versions $(dpkg-query -f='${Version}\n' -W nvidia-driver-${NVIDIA_DRIVER_VERSION}) ge $line ; then
           echo "$line"
           break
        fi
      done)
      
#      apt-get install -y cuda-drivers-${NVIDIA_DRIVER_VERSION} cuda-drivers=${CUDA_DRIVER_VERSION}
      apt install -y cuda-drivers-${NVIDIA_DRIVER_VERSION}=${CUDA_DRIVER_VERSION} cuda-drivers=${CUDA_DRIVER_VERSION}
      
      apt-get remove dkms && apt-mark hold dkms
      
      # the $line should be "cuda-runtime-12-0,cuda-drivers 525.85.12"
      CUDA_VERSION=$(apt-cache showpkg cuda-drivers | grep -o 'cuda-runtime-[0-9][0-9]-[0-9],cuda-drivers [0-9\.]*' | while read line; do
         if dpkg --compare-versions ${CUDA_DRIVER_VERSION} ge $(echo $line | grep -Eo '[[:digit:]]+\.[[:digit:]]+') ; then
             echo $(echo $line | grep -Eo '[[:digit:]]+-[[:digit:]]')
             break
         fi
      done)
      
      apt install -y cuda-${CUDA_VERSION}
      
    else
      curl -fsSL --retry-connrefused --retry 3 --retry-max-time 5 \
      "${NVIDIA_UBUNTU_REPO_KEY_PACKAGE}" -o /tmp/cuda-keyring.deb
      dpkg -i "/tmp/cuda-keyring.deb"
      curl -fsSL --retry-connrefused --retry 3 --retry-max-time 5 \
        "${NVIDIA_UBUNTU_REPO_CUDA_PIN}" -o /etc/apt/preferences.d/cuda-repository-pin-600

      add-apt-repository "deb ${NVIDIA_UBUNTU_REPO_URL} /"
      execute_with_retries "apt-get update"

      if [[ -n "${CUDA_VERSION}" ]]; then
        local -r cuda_package=cuda-toolkit-${CUDA_VERSION//./-}
      else
        local -r cuda_package=cuda-toolkit
      fi
      # Without --no-install-recommends this takes a very long time.
      execute_with_retries "apt-get install -y -q --no-install-recommends cuda-drivers-${DEFAULT_NVIDIA_DEBIAN_GPU_DRIVER_VERSION_PREFIX}"
      execute_with_retries "apt-get install -y -q --no-install-recommends ${cuda_package}"  
    fi
  elif [[ ${OS_NAME} == rocky ]]; then
    execute_with_retries "dnf config-manager --add-repo ${NVIDIA_ROCKY_REPO_URL}"
    execute_with_retries "dnf clean all"
    # Always install the latest cuda/driver version because old driver version 495 has issues
    execute_with_retries "dnf install -y -q nvidia-driver nvidia-settings cuda-driver"
    modprobe nvidia
  else
    echo "Unsupported OS: '${OS_NAME}'"
    exit 1
  fi
  ldconfig
  echo "NVIDIA GPU driver provided by NVIDIA was installed successfully"
}

# Collects 'gpu_utilization' and 'gpu_memory_utilization' metrics
function install_gpu_agent() {
  download_agent
  install_agent_dependency
  start_agent_service
}

function download_agent(){
  if [[ ${OS_NAME} == rocky ]]; then
    execute_with_retries "dnf -y -q install git"
  else
    execute_with_retries "apt-get install git -y"
  fi
  mkdir -p /opt/google
  chmod 777 /opt/google
  cd /opt/google
  execute_with_retries "git clone https://github.com/GoogleCloudPlatform/compute-gpu-monitoring.git"
}

function install_agent_dependency(){
  cd /opt/google/compute-gpu-monitoring/linux
  python3 -m venv venv
  venv/bin/pip install wheel
  venv/bin/pip install -Ur requirements.txt
}

function start_agent_service(){
  cp /opt/google/compute-gpu-monitoring/linux/systemd/google_gpu_monitoring_agent_venv.service /lib/systemd/system
  systemctl daemon-reload
  systemctl --no-reload --now enable /lib/systemd/system/google_gpu_monitoring_agent_venv.service
}

function set_hadoop_property() {
  local -r config_file=$1
  local -r property=$2
  local -r value=$3
  bdconfig set_property \
    --configuration_file "${HADOOP_CONF_DIR}/${config_file}" \
    --name "${property}" --value "${value}" \
    --clobber
}

function configure_yarn() {
  if [[ ! -f ${HADOOP_CONF_DIR}/resource-types.xml ]]; then
    printf '<?xml version="1.0" ?>\n<configuration/>' >"${HADOOP_CONF_DIR}/resource-types.xml"
  fi
  set_hadoop_property 'resource-types.xml' 'yarn.resource-types' 'yarn.io/gpu'

  set_hadoop_property 'capacity-scheduler.xml' \
    'yarn.scheduler.capacity.resource-calculator' \
    'org.apache.hadoop.yarn.util.resource.DominantResourceCalculator'

  set_hadoop_property 'yarn-site.xml' 'yarn.resource-types' 'yarn.io/gpu'
}

# This configuration should be applied only if GPU is attached to the node
function configure_yarn_nodemanager() {
  set_hadoop_property 'yarn-site.xml' 'yarn.nodemanager.resource-plugins' 'yarn.io/gpu'
  set_hadoop_property 'yarn-site.xml' \
    'yarn.nodemanager.resource-plugins.gpu.allowed-gpu-devices' 'auto'
  set_hadoop_property 'yarn-site.xml' \
    'yarn.nodemanager.resource-plugins.gpu.path-to-discovery-executables' $NVIDIA_SMI_PATH
  set_hadoop_property 'yarn-site.xml' \
    'yarn.nodemanager.linux-container-executor.cgroups.mount' 'true'
  set_hadoop_property 'yarn-site.xml' \
    'yarn.nodemanager.linux-container-executor.cgroups.mount-path' '/sys/fs/cgroup'
  set_hadoop_property 'yarn-site.xml' \
    'yarn.nodemanager.linux-container-executor.cgroups.hierarchy' 'yarn'
  set_hadoop_property 'yarn-site.xml' \
    'yarn.nodemanager.container-executor.class' \
    'org.apache.hadoop.yarn.server.nodemanager.LinuxContainerExecutor'
  set_hadoop_property 'yarn-site.xml' 'yarn.nodemanager.linux-container-executor.group' 'yarn'

}

function configure_gpu_exclusive_mode() {
  # check if running spark 3, if not, enable GPU exclusive mode
  local spark_version
  spark_version=$(spark-submit --version 2>&1 | sed -n 's/.*version[[:blank:]]\+\([0-9]\+\.[0-9]\).*/\1/p' | head -n1)
  if [[ ${spark_version} != 3.* ]]; then
    # include exclusive mode on GPU
    nvidia-smi -c EXCLUSIVE_PROCESS
  fi
}

function fetch_mig_scripts() {
  mkdir -p /usr/local/yarn-mig-scripts
  chmod 755 /usr/local/yarn-mig-scripts
  wget -P /usr/local/yarn-mig-scripts/ https://raw.githubusercontent.com/NVIDIA/spark-rapids-examples/branch-22.10/examples/MIG-Support/yarn-unpatched/scripts/nvidia-smi
  wget -P /usr/local/yarn-mig-scripts/ https://raw.githubusercontent.com/NVIDIA/spark-rapids-examples/branch-22.10/examples/MIG-Support/yarn-unpatched/scripts/mig2gpu.sh
  chmod 755 /usr/local/yarn-mig-scripts/*
}

function configure_gpu_script() {
  # Download GPU discovery script
  local -r spark_gpu_script_dir='/usr/lib/spark/scripts/gpu'
  mkdir -p ${spark_gpu_script_dir}
  # need to update the getGpusResources.sh script to look for MIG devices since if multiple GPUs nvidia-smi still
  # lists those because we only disable the specific GIs via CGROUPs. Here we just create it based off of:
  # https://raw.githubusercontent.com/apache/spark/master/examples/src/main/scripts/getGpusResources.sh
  echo '
#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
NUM_MIG_DEVICES=$(nvidia-smi -L | grep MIG | wc -l)
ADDRS=$(nvidia-smi --query-gpu=index --format=csv,noheader | sed -e '\'':a'\'' -e '\''N'\'' -e'\''$!ba'\'' -e '\''s/\n/","/g'\'')
if [ $NUM_MIG_DEVICES -gt 0 ]; then
  MIG_INDEX=$(( $NUM_MIG_DEVICES - 1 ))
  ADDRS=$(seq -s '\''","'\'' 0 $MIG_INDEX)
fi
echo {\"name\": \"gpu\", \"addresses\":[\"$ADDRS\"]}
' > ${spark_gpu_script_dir}/getGpusResources.sh

  chmod a+rwx -R ${spark_gpu_script_dir}
}

function configure_gpu_isolation() {
  # enable GPU isolation
  sed -i "s/yarn\.nodemanager\.linux\-container\-executor\.group\=.*$/yarn\.nodemanager\.linux\-container\-executor\.group\=yarn/g" "${HADOOP_CONF_DIR}/container-executor.cfg"
  if [[ $IS_MIG_ENABLED -ne 0 ]]; then
    # configure the container-executor.cfg to have major caps
    printf '\n[gpu]\nmodule.enabled=true\ngpu.major-device-number=%s\n\n[cgroups]\nroot=/sys/fs/cgroup\nyarn-hierarchy=yarn\n' $MIG_MAJOR_CAPS >> "${HADOOP_CONF_DIR}/container-executor.cfg"
    printf 'export MIG_AS_GPU_ENABLED=1\n' >> "${HADOOP_CONF_DIR}/yarn-env.sh"
    printf 'export ENABLE_MIG_GPUS_FOR_CGROUPS=1\n' >> "${HADOOP_CONF_DIR}/yarn-env.sh"
  else
    printf '\n[gpu]\nmodule.enabled=true\n[cgroups]\nroot=/sys/fs/cgroup\nyarn-hierarchy=yarn\n' >> "${HADOOP_CONF_DIR}/container-executor.cfg"
  fi

  # Configure a systemd unit to ensure that permissions are set on restart
  cat >/etc/systemd/system/dataproc-cgroup-device-permissions.service<<EOF
[Unit]
Description=Set permissions to allow YARN to access device directories

[Service]
ExecStart=/bin/bash -c "chmod a+rwx -R /sys/fs/cgroup/cpu,cpuacct; chmod a+rwx -R /sys/fs/cgroup/devices"

[Install]
WantedBy=multi-user.target
EOF

  systemctl enable dataproc-cgroup-device-permissions
  systemctl start dataproc-cgroup-device-permissions
}

function setup_gpu_yarn() {

  if [[ ${OS_NAME} == debian ]] || [[ ${OS_NAME} == ubuntu ]]; then
    export DEBIAN_FRONTEND=noninteractive
    execute_with_retries "apt-get update"
    execute_with_retries "apt-get install -y -q pciutils"
  elif [[ ${OS_NAME} == rocky ]] ; then
    execute_with_retries "dnf -y -q install pciutils"
  else
    echo "Unsupported OS: '${OS_NAME}'"
    exit 1
  fi

  # This configuration should be ran on all nodes
  # regardless if they have attached GPUs
  configure_yarn

  # Detect NVIDIA GPU
  if (lspci | grep -q NVIDIA); then
    # if this is called without the MIG script then the drivers are not installed
    if (/usr/bin/nvidia-smi --query-gpu=mig.mode.current --format=csv,noheader | uniq | wc -l); then
      NUM_MIG_GPUS=`/usr/bin/nvidia-smi --query-gpu=mig.mode.current --format=csv,noheader | uniq | wc -l`
      if [[ $NUM_MIG_GPUS -eq 1 ]]; then
        if (/usr/bin/nvidia-smi --query-gpu=mig.mode.current --format=csv,noheader | grep Enabled); then
          IS_MIG_ENABLED=1
          NVIDIA_SMI_PATH='/usr/local/yarn-mig-scripts/'
          MIG_MAJOR_CAPS=`grep nvidia-caps /proc/devices | cut -d ' ' -f 1`
          fetch_mig_scripts
        fi
      fi
    fi

    if [[ ${OS_NAME} == debian ]] || [[ ${OS_NAME} == ubuntu ]]; then
      execute_with_retries "apt-get install -y -q 'linux-headers-$(uname -r)'"
    elif [[ ${OS_NAME} == rocky ]]; then
      echo "kernel devel and headers not required on rocky.  installing from binary"
    fi

    # if mig is enabled drivers would have already been installed
    if [[ $IS_MIG_ENABLED -eq 0 ]]; then
      install_nvidia_gpu_driver

      #Install GPU metrics collection in Stackdriver if needed
      if [[ ${INSTALL_GPU_AGENT} == true ]]; then
        install_gpu_agent
        echo 'GPU metrics agent successfully deployed.'
      else
        echo 'GPU metrics agent will not be installed.'
      fi
      configure_gpu_exclusive_mode
    fi

    configure_yarn_nodemanager
    configure_gpu_script
    configure_gpu_isolation
  elif [[ "${ROLE}" == "Master" ]]; then
    configure_yarn_nodemanager
    configure_gpu_script
  fi

  # Restart YARN services if they are running already
  for svc in resourcemanager nodemanager; do
    if [[ $(systemctl show hadoop-yarn-${svc}.service -p SubState --value) == 'running' ]]; then
      systemctl restart hadoop-yarn-${svc}.service
    fi
  done
}

function upgrade_kernel() {
  # Determine which kernel is installed
  if [[ "${OS_NAME}" == "debian" ]]; then
    CURRENT_KERNEL_VERSION=`cat /proc/version  | perl -ne 'print( / Debian (\S+) / )'`
  elif [[ "${OS_NAME}" == "ubuntu" ]]; then
    CURRENT_KERNEL_VERSION=`cat /proc/version | perl -ne 'print( /^Linux version (\S+) / )'`
  elif [[ ${OS_NAME} == rocky ]]; then
    KERN_VER=$(yum info --installed kernel | awk '/^Version/ {print $3}')
    KERN_REL=$(yum info --installed kernel | awk '/^Release/ {print $3}')
    # something like 4.18.0-425.10.1.el8_7
    CURRENT_KERNEL_VERSION="${KERN_VER}-${KERN_REL}"
  else
    echo "unsupported OS: ${OS_NAME}!"
    exit -1
  fi

  # Get latest version available in repos
  if [[ "${OS_NAME}" == "debian" ]]; then
    apt-get -qq update
    TARGET_VERSION=$(apt-cache show --no-all-versions linux-image-amd64 | awk '/^Version/ {print $2}')
  elif [[ "${OS_NAME}" == "ubuntu" ]]; then
    apt-get -qq update
    LATEST_VERSION=$(apt-cache show --no-all-versions linux-image-gcp | awk '/^Version/ {print $2}')
    TARGET_VERSION=`echo ${LATEST_VERSION} | perl -ne 'printf(q{%s-%s-gcp},/(\d+\.\d+\.\d+)\.(\d+)/)'`
  elif [[ "${OS_NAME}" == "rocky" ]]; then
    if yum info --available kernel ; then
      KERN_VER=$(yum info --available kernel | awk '/^Version/ {print $3}')
      KERN_REL=$(yum info --available kernel | awk '/^Release/ {print $3}')
      TARGET_VERSION="${KERN_VER}-${KERN_REL}"
    else
      TARGET_VERSION="${CURRENT_KERNEL_VERSION}"
    fi
  fi

  # Skip this script if we are already on the target version
  if [[ "${CURRENT_KERNEL_VERSION}" == "${TARGET_VERSION}" ]]; then
    echo "target kernel version [${TARGET_VERSION}] is installed"

    # Reboot may have interrupted dpkg.  Bring package system to a good state
    if [[ "${OS_NAME}" == "debian" || "${OS_NAME}" == "ubuntu" ]]; then
      dpkg --configure -a
    fi

    return 0
  fi

  # Install the latest kernel
  if [[ ${OS_NAME} == debian ]]; then
    apt-get install -y linux-image-amd64
  elif [[ "${OS_NAME}" == "ubuntu" ]]; then
    apt-get install -y linux-image-gcp
  elif [[ "${OS_NAME}" == "rocky" ]]; then
    dnf -y -q install kernel
  fi

  # Make it possible to reboot before init actions are complete - #1033
  DP_ROOT=/usr/local/share/google/dataproc
  STARTUP_SCRIPT="${DP_ROOT}/startup-script.sh"
  POST_HDFS_STARTUP_SCRIPT="${DP_ROOT}/post-hdfs-startup-script.sh"
  
  for startup_script in ${STARTUP_SCRIPT} ${POST_HDFS_STARTUP_SCRIPT} ; do
    sed -i -e 's:/usr/bin/env bash:/usr/bin/env bash\nexit 0:' ${startup_script}
  done

  cp /var/log/dataproc-initialization-script-0.log /var/log/dataproc-initialization-script-0.log.0

  systemctl reboot
}

function main() {

  if [[ "${OS_NAME}" == "rocky" ]]; then
    if dnf list kernel-devel-$(uname -r) && dnf list kernel-headers-$(uname -r); then
      echo "kernel devel and headers packages are available.  Proceed without kernel upgrade."
    else
      upgrade_kernel
    fi
  fi
  setup_gpu_yarn
  if [[ "${RUNTIME}" == "SPARK" ]]; then
    install_spark_rapids
    configure_spark
    echo "RAPIDS initialized with Spark runtime"
  else
    echo "Unsupported RAPIDS Runtime: ${RUNTIME}"
    exit 1
  fi

  for svc in resourcemanager nodemanager; do
    if [[ $(systemctl show hadoop-yarn-${svc}.service -p SubState --value) == 'running' ]]; then
      systemctl restart hadoop-yarn-${svc}.service
    fi
  done
}

main
