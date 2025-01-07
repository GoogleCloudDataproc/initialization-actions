#!/bin/bash

# Copyright 2019,2020,2021,2022,2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This initialization action script will install rapids on a Dataproc
# cluster.

set -euxo pipefail

function os_id()       { grep '^ID=' /etc/os-release | cut -d= -f2 | xargs ; }
function is_ubuntu()   { [[ "$(os_id)" == 'ubuntu' ]] ; }
function is_ubuntu18() { is_ubuntu && [[ "$(os_version)" == '18.04'* ]] ; }
function is_debian()   { [[ "$(os_id)" == 'debian' ]] ; }
function is_debuntu()  { is_debian || is_ubuntu ; }

function print_metadata_value() {
  local readonly tmpfile=$(mktemp)
  http_code=$(curl -f "${1}" -H "Metadata-Flavor: Google" -w "%{http_code}" \
    -s -o ${tmpfile} 2>/dev/null)
  local readonly return_code=$?
  # If the command completed successfully, print the metadata value to stdout.
  if [[ ${return_code} == 0 && ${http_code} == 200 ]]; then
    cat ${tmpfile}
  fi
  rm -f ${tmpfile}
  return ${return_code}
}

function print_metadata_value_if_exists() {
  local return_code=1
  local readonly url=$1
  print_metadata_value ${url}
  return_code=$?
  return ${return_code}
}

function get_metadata_value() {
  set +x
  local readonly varname=$1
  local -r MDS_PREFIX=http://metadata.google.internal/computeMetadata/v1
  # Print the instance metadata value.
  print_metadata_value_if_exists ${MDS_PREFIX}/instance/${varname}
  return_code=$?
  # If the instance doesn't have the value, try the project.
  if [[ ${return_code} != 0 ]]; then
    print_metadata_value_if_exists ${MDS_PREFIX}/project/${varname}
    return_code=$?
  fi
  set -x
  return ${return_code}
}

function get_metadata_attribute() (
  set +x
  local -r attribute_name="$1"
  local -r default_value="${2:-}"
  get_metadata_value "attributes/${attribute_name}" || echo -n "${default_value}"
)

function is_cuda12() { [[ "${CUDA_VERSION%%.*}" == "12" ]] ; }
function is_cuda11() { [[ "${CUDA_VERSION%%.*}" == "11" ]] ; }

function execute_with_retries() {
  local -r cmd="$*"
  for i in {0..9} ; do
    if eval "$cmd"; then
      return 0 ; fi
    sleep 5
  done
  echo "Cmd '${cmd}' failed."
  return 1
}

function configure_dask_yarn() {
  readonly DASK_YARN_CONFIG_DIR=/etc/dask/
  readonly DASK_YARN_CONFIG_FILE=${DASK_YARN_CONFIG_DIR}/config.yaml
  # Minimal custom configuration is required for this
  # setup. Please see https://yarn.dask.org/en/latest/quickstart.html#usage
  # for information on tuning Dask-Yarn environments.
  mkdir -p "${DASK_YARN_CONFIG_DIR}"

  cat <<EOF >"${DASK_YARN_CONFIG_FILE}"
# Config file for Dask Yarn.
#
# These values are joined on top of the default config, found at
# https://yarn.dask.org/en/latest/configuration.html#default-configuration

yarn:
  environment: python://${DASK_CONDA_ENV}/bin/python

  worker:
    count: 2
    gpus: 1
    class: "dask_cuda.CUDAWorker"
EOF
}

function install_systemd_dask_worker() {
  echo "Installing systemd Dask Worker service..."
  local -r dask_worker_local_dir="/tmp/${DASK_WORKER_SERVICE}"

  mkdir -p "${dask_worker_local_dir}"

  local DASK_WORKER_LAUNCHER="/usr/local/bin/${DASK_WORKER_SERVICE}-launcher.sh"

  cat <<EOF >"${DASK_WORKER_LAUNCHER}"
#!/bin/bash
LOGFILE="/var/log/${DASK_WORKER_SERVICE}.log"
nvidia-smi -c DEFAULT
echo "dask-cuda-worker starting, logging to \${LOGFILE}"
${DASK_CONDA_ENV}/bin/dask-cuda-worker "${MASTER}:8786" --local-directory="${dask_worker_local_dir}" --memory-limit=auto >> "\${LOGFILE}" 2>&1
EOF

  chmod 750 "${DASK_WORKER_LAUNCHER}"

  local -r dask_service_file="/usr/lib/systemd/system/${DASK_WORKER_SERVICE}.service"
  cat <<EOF >"${dask_service_file}"
[Unit]
Description=Dask Worker Service
[Service]
Type=simple
Restart=on-failure
ExecStart=/bin/bash -c 'exec ${DASK_WORKER_LAUNCHER}'
[Install]
WantedBy=multi-user.target
EOF
  chmod a+r "${dask_service_file}"

  systemctl daemon-reload

  # Enable the service
  if [[ "${ROLE}" != "Master" ]]; then
    enable_worker_service="1"
  else
     local RUN_WORKER_ON_MASTER=$(get_metadata_attribute dask-cuda-worker-on-master 'true')
    # Enable service on single-node cluster (no workers)
    local worker_count="$(get_metadata_attribute dataproc-worker-count)"
    if [[ "${worker_count}" == "0" || "${RUN_WORKER_ON_MASTER}" == "true" ]]; then
      enable_worker_service="1"
    fi
  fi

  if [[ "${enable_worker_service}" == "1" ]]; then
    systemctl enable "${DASK_WORKER_SERVICE}"
    systemctl restart "${DASK_WORKER_SERVICE}"
  fi
}

function install_systemd_dask_scheduler() {
  # only run scheduler on primary master
  if [[ "$(hostname -s)" != "${MASTER}" ]]; then return ; fi
  echo "Installing systemd Dask Scheduler service..."
  local -r dask_scheduler_local_dir="/tmp/${DASK_SCHEDULER_SERVICE}"

  mkdir -p "${dask_scheduler_local_dir}"

  local DASK_SCHEDULER_LAUNCHER="/usr/local/bin/${DASK_SCHEDULER_SERVICE}-launcher.sh"

  cat <<EOF >"${DASK_SCHEDULER_LAUNCHER}"
#!/bin/bash
LOGFILE="/var/log/${DASK_SCHEDULER_SERVICE}.log"
echo "dask scheduler starting, logging to \${LOGFILE}"
${DASK_CONDA_ENV}/bin/dask scheduler >> "\${LOGFILE}" 2>&1
EOF

  chmod 750 "${DASK_SCHEDULER_LAUNCHER}"

  local -r dask_service_file="/usr/lib/systemd/system/${DASK_SCHEDULER_SERVICE}.service"
  cat <<EOF >"${dask_service_file}"
[Unit]
Description=Dask Scheduler Service
[Service]
Type=simple
Restart=on-failure
ExecStart=/bin/bash -c 'exec ${DASK_SCHEDULER_LAUNCHER}'
[Install]
WantedBy=multi-user.target
EOF
  chmod a+r "${dask_service_file}"

  systemctl daemon-reload

  # Enable the service
  systemctl enable "${DASK_SCHEDULER_SERVICE}"
}

function install_systemd_dask_service() {
  install_systemd_dask_scheduler
  install_systemd_dask_worker
}

function restart_knox() {
  systemctl stop knox
  rm -rf "${KNOX_HOME}/data/deployments/*"
  systemctl start knox
}

function configure_knox_for_dask() {
  if [[ ! -d "${KNOX_HOME}" ]]; then
    echo "Skip configuring Knox rules for Dask"
    return 0
  fi

  local DASK_UI_PORT=8787
  if [[ -f /etc/knox/conf/topologies/default.xml ]]; then
    sed -i \
      "/<\/topology>/i <service><role>DASK<\/role><url>http://localhost:${DASK_UI_PORT}<\/url><\/service> <service><role>DASKWS<\/role><url>ws:\/\/${MASTER}:${DASK_UI_PORT}<\/url><\/service>" \
      /etc/knox/conf/topologies/default.xml
  fi

  mkdir -p "${KNOX_DASK_DIR}"

  cat >"${KNOX_DASK_DIR}/service.xml" <<'EOF'
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>

<service role="DASK" name="dask" version="0.1.0">
  <policies>
    <policy role="webappsec"/>
    <policy role="authentication" name="Anonymous"/>
    <policy role="rewrite"/>
    <policy role="authorization"/>
  </policies>

  <routes>
    <!-- Javascript paths -->
    <route path="/dask/**/*.js">
      <rewrite apply="DASK/dask/inbound/js/dask" to="request.url"/>
      <rewrite apply="DASK/dask/outbound/js" to="response.body"/>
    </route>
    <route path="/dask/**/*.js?**">
      <rewrite apply="DASK/dask/inbound/js/dask" to="request.url"/>
      <rewrite apply="DASK/dask/outbound/js" to="response.body"/>
    </route>

    <!-- CSS paths -->
    <route path="/dask/**/*.css">
      <rewrite apply="DASK/dask/inbound/css/dask" to="request.url"/>
    </route>

    <!-- General path routing -->
    <route path="/dask">
      <rewrite apply="DASK/dask/inbound/root" to="request.url"/>
      <rewrite apply="DASK/dask/outbound/headers" to="response.headers"/>
    </route>
    <route path="/dask/**">
      <rewrite apply="DASK/dask/inbound/root/path" to="request.url"/>
      <rewrite apply="DASK/dask/outbound/headers" to="response.headers"/>
      <rewrite apply="DASK/dask/outbound/logs" to="response.body"/>
    </route>
    <route path="/dask/**?**">
      <rewrite apply="DASK/dask/inbound/root/query" to="request.url"/>
      <rewrite apply="DASK/dask/outbound/headers" to="response.headers"/>
      <rewrite apply="DASK/dask/outbound/logs" to="response.body"/>
    </route>
  </routes>
  <dispatch classname="org.apache.knox.gateway.dispatch.PassAllHeadersNoChunkedPostDispatch"/>
</service>
EOF

  cat >"${KNOX_DASK_DIR}/rewrite.xml" <<'EOF'
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>

<rules>
  <rule dir="IN" name="DASK/dask/inbound/js/dask" pattern="http://*:*/**/dask/{**}?{**}">
    <rewrite template="{$serviceUrl[DASK]}/{**}?{**}"/>
  </rule>
  <rule dir="IN" name="DASK/dask/inbound/root" pattern="http://*:*/**/dask">
    <rewrite template="{$serviceUrl[DASK]}"/>
  </rule>
  <rule dir="IN" name="DASK/dask/inbound/root/path" pattern="http://*:*/**/dask/{**}">
    <rewrite template="{$serviceUrl[DASK]}/{**}"/>
  </rule>
  <rule dir="IN" name="DASK/dask/inbound/root/query" pattern="http://*:*/**/dask/{**}?{**}">
    <rewrite template="{$serviceUrl[DASK]}/{**}?{**}"/>
  </rule>
  <rule dir="IN" name="DASK/dask/inbound/css/dask" pattern="http://*:*/**/dask/{**}?{**}">
    <rewrite template="{$serviceUrl[DASK]}/{**}?{**}"/>
  </rule>
  <!-- without the /gateway/default prefix -->
  <rule dir="IN" name="DASK/dask/inbound/root/noprefix" pattern="http://*:*/dask">
    <rewrite template="{$serviceUrl[DASK]}"/>
  </rule>

  <rule dir="OUT" name="DASK/dask/outbound/logs" pattern="/logs">
    <rewrite template="{$frontend[path]}/dask/info/logs"/>
  </rule>

  <!-- Rewrite redirect responses Location header -->
  <filter name="DASK/dask/outbound/headers">
    <content type="application/x-http-headers">
      <apply path="Location" rule="DASK/dask/outbound/headers/location"/>
    </content>
  </filter>

  <rule dir="OUT" name="DASK/dask/outbound/headers/location" flow="OR">
    <match pattern="*://*:*/">
      <rewrite template="{$frontend[path]}/dask/"/>
    </match>
    <match pattern="*://*:*/{**}">
      <rewrite template="{$frontend[path]}/dask/{**}"/>
    </match>
    <match pattern="*://*:*/{**}?{**}">
      <rewrite template="{$frontend[path]}/dask/{**}?{**}"/>
    </match>
    <match pattern="/{**}">
      <rewrite template="{$frontend[path]}/dask/{**}"/>
    </match>
    <match pattern="/{**}?{**}">
      <rewrite template="{$frontend[path]}/dask/{**}?{**}"/>
    </match>
  </rule>
</rules>
EOF

  mkdir -p "${KNOX_DASKWS_DIR}"

  cat >"${KNOX_DASKWS_DIR}/service.xml" <<'EOF'
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>

<service role="DASKWS" name="daskws" version="0.1.0">
  <policies>
    <policy role="webappsec"/>
    <policy role="authentication" name="Anonymous"/>
    <policy role="rewrite"/>
    <policy role="authorization"/>
  </policies>

  <routes>

    <route path="/dask/**/ws">
      <rewrite apply="DASKWS/daskws/inbound/ws" to="request.url"/>
    </route>

  </routes>
  <dispatch classname="org.apache.knox.gateway.dispatch.PassAllHeadersNoChunkedPostDispatch"/>
</service>
EOF

  cat >"${KNOX_DASKWS_DIR}/rewrite.xml" <<'EOF'
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>

<rules>
  <rule dir="IN" name="DASKWS/daskws/inbound/ws" pattern="ws://*:*/**/dask/{**}/ws">
    <rewrite template="{$serviceUrl[DASKWS]}/{**}/ws"/>
  </rule>
</rules>
EOF

  chown -R knox:knox "${KNOX_DASK_DIR}" "${KNOX_DASKWS_DIR}"

  # Do not restart knox during pre-init script run
  if [[ -n "${ROLE}" ]]; then
    restart_knox
  fi
}

function configure_fluentd_for_dask() {
  if [[ "$(hostname -s)" == "${MASTER}" ]]; then
    cat >/etc/google-fluentd/config.d/dataproc-dask.conf <<EOF
# Fluentd config for Dask logs

# Dask scheduler
<source>
  @type tail
  path /var/log/dask-scheduler.log
  pos_file /var/tmp/fluentd.dataproc.dask.scheduler.pos
  read_from_head true
  tag google.dataproc.dask-scheduler
  <parse>
    @type none
  </parse>
</source>

<filter google.dataproc.dask-scheduler>
  @type record_transformer
  <record>
    filename dask-scheduler.log
  </record>
</filter>
EOF
  fi

  if [[ "${enable_worker_service}" == "1" ]]; then
    cat >>/etc/google-fluentd/config.d/dataproc-dask.conf <<EOF
# Dask worker
<source>
  @type tail
  path /var/log/dask-worker.log
  pos_file /var/tmp/fluentd.dataproc.dask.worker.pos
  read_from_head true
  tag google.dataproc.dask-worker
  <parse>
    @type none
  </parse>
</source>

<filter google.dataproc.dask-worker>
  @type record_transformer
  <record>
    filename dask-worker.log
  </record>
</filter>
EOF
  fi

  systemctl restart google-fluentd
}

function install_dask_rapids() {
  if is_cuda12 ; then
    local python_spec="python>=3.11"
    local cuda_spec="cuda-version>=12,<13"
    local dask_spec="dask>=2024.7"
    local numba_spec="numba"
  elif is_cuda11 ; then
    local python_spec="python>=3.9"
    local cuda_spec="cuda-version>=11,<12.0a0"
    local dask_spec="dask"
    local numba_spec="numba"
  fi

  rapids_spec="rapids>=${RAPIDS_VERSION}"
  CONDA_PACKAGES=()
  if [[ "${DASK_RUNTIME}" == 'yarn' ]]; then
    # Pin `distributed` and `dask` package versions to old release
    # because `dask-yarn` 0.9 uses skein in a way which
    # is not compatible with `distributed` package 2022.2 and newer:
    # https://github.com/dask/dask-yarn/issues/155

    dask_spec="dask<2022.2"
    python_spec="python>=3.7,<3.8.0a0"
    rapids_spec="rapids<=24.05"
    if is_ubuntu18 ; then
      # the libuuid.so.1 distributed with fiona 1.8.22 dumps core when calling uuid_generate_time_generic
      CONDA_PACKAGES+=("fiona<1.8.22")
    fi
    CONDA_PACKAGES+=('dask-yarn=0.9' "distributed<2022.2")
  fi

  CONDA_PACKAGES+=(
    "${cuda_spec}"
    "${rapids_spec}"
    "${dask_spec}"
    "dask-bigquery"
    "dask-ml"
    "dask-sql"
    "cudf"
    "${numba_spec}"
  )

  # Install cuda, rapids, dask
  mamba="/opt/conda/miniconda3/bin/mamba"
  conda="/opt/conda/miniconda3/bin/conda"

  "${conda}" remove -n dask --all || echo "unable to remove conda environment [dask]"

  ( set +e
  local is_installed="0"
  for installer in "${mamba}" "${conda}" ; do
    test -d "${DASK_CONDA_ENV}" || \
      time "${installer}" "create" -m -n 'dask-rapids' -y --no-channel-priority \
      -c 'conda-forge' -c 'nvidia' -c 'rapidsai'  \
      ${CONDA_PACKAGES[*]} \
      "${python_spec}" \
      > "${install_log}" 2>&1 && retval=$? || { retval=$? ; cat "${install_log}" ; }
    sync
    if [[ "$retval" == "0" ]] ; then
      is_installed="1"
      break
    fi
    "${conda}" config --set channel_priority flexible
  done
  if [[ "${is_installed}" == "0" ]]; then
    echo "failed to install dask"
    return 1
  fi
  )
}

function main() {
  # Install Dask with RAPIDS
  install_dask_rapids

  # In "standalone" mode, Dask relies on a systemd unit to launch.
  # In "yarn" mode, it relies a config.yaml file.
  if [[ "${DASK_RUNTIME}" == "yarn" ]]; then
    # Create Dask YARN config file
    configure_dask_yarn
  else
    # Create Dask service
    install_systemd_dask_service

    if [[ "$(hostname -s)" == "${MASTER}" ]]; then
      systemctl start "${DASK_SCHEDULER_SERVICE}"
      systemctl status "${DASK_SCHEDULER_SERVICE}"
    fi

    echo "Starting Dask 'standalone' cluster..."
    if [[ "${enable_worker_service}" == "1" ]]; then
      systemctl start "${DASK_WORKER_SERVICE}"
      systemctl status "${DASK_WORKER_SERVICE}"
    fi

    configure_knox_for_dask

    local DASK_CLOUD_LOGGING="$(get_metadata_attribute dask-cloud-logging || echo 'false')"
    if [[ "${DASK_CLOUD_LOGGING}" == "true" ]]; then
      configure_fluentd_for_dask
    fi
  fi

  echo "Dask RAPIDS for ${DASK_RUNTIME} successfully initialized."
  if [[ "${ROLE}" == "Master" ]]; then
    systemctl restart hadoop-yarn-resourcemanager.service
    # Restart NodeManager on Master as well if this is a single-node-cluster.
    if systemctl list-units | grep hadoop-yarn-nodemanager; then
      systemctl restart hadoop-yarn-nodemanager.service
    fi
  else
    systemctl restart hadoop-yarn-nodemanager.service
  fi
}

function exit_handler() (
  set +e
  echo "Exit handler invoked"

  # Free conda cache
  /opt/conda/miniconda3/bin/conda clean -a > /dev/null 2>&1

  # Clear pip cache
  pip cache purge || echo "unable to purge pip cache"

  # remove the tmpfs conda pkgs_dirs
  if [[ -d /mnt/shm ]] ; then /opt/conda/miniconda3/bin/conda config --remove pkgs_dirs /mnt/shm ; fi

  # Clean up shared memory mounts
  for shmdir in /var/cache/apt/archives /var/cache/dnf /mnt/shm ; do
    if grep -q "^tmpfs ${shmdir}" /proc/mounts ; then
      rm -rf ${shmdir}/*
      umount -f ${shmdir}
    fi
  done

  # Clean up OS package cache ; re-hold systemd package
  if is_debuntu ; then
    apt-get -y -qq clean
    apt-get -y -qq autoremove
  else
    dnf clean all
  fi

  # print disk usage statistics
  if is_debuntu ; then
    # Rocky doesn't have sort -h and fails when the argument is passed
    du --max-depth 3 -hx / | sort -h | tail -10
  fi

  # Process disk usage logs from installation period
  rm -f "${tmpdir}/keep-running-df"
  sleep 6s
  # compute maximum size of disk during installation
  # Log file contains logs like the following (minus the preceeding #):
#Filesystem      Size  Used Avail Use% Mounted on
#/dev/vda2       6.8G  2.5G  4.0G  39% /
  df -h / | tee -a "${tmpdir}/disk-usage.log"
  perl -e '$max=( sort
                   map { (split)[2] =~ /^(\d+)/ }
                  grep { m:^/: } <STDIN> )[-1];
print( "maximum-disk-used: $max", $/ );' < "${tmpdir}/disk-usage.log"

  echo "exit_handler has completed"

  # zero free disk space
  if [[ -n "$(get_metadata_attribute creating-image)" ]]; then
    dd if=/dev/zero of=/zero ; sync ; rm -f /zero
  fi

  return 0
)

function prepare_to_install(){
  readonly DEFAULT_CUDA_VERSION="12.4"
  CUDA_VERSION=$(get_metadata_attribute 'cuda-version' ${DEFAULT_CUDA_VERSION})
  readonly CUDA_VERSION

  readonly ROLE=$(get_metadata_attribute dataproc-role)
  readonly MASTER=$(get_metadata_attribute dataproc-master)

  # RAPIDS config
  RAPIDS_RUNTIME=$(get_metadata_attribute 'rapids-runtime' 'DASK')
  readonly RAPIDS_RUNTIME

  readonly DEFAULT_DASK_RAPIDS_VERSION="24.08"
  readonly RAPIDS_VERSION=$(get_metadata_attribute 'rapids-version' ${DEFAULT_DASK_RAPIDS_VERSION})

  # Dask config
  DASK_RUNTIME="$(get_metadata_attribute dask-runtime || echo 'standalone')"
  readonly DASK_RUNTIME
  readonly DASK_SERVICE=dask-cluster
  readonly DASK_WORKER_SERVICE=dask-worker
  readonly DASK_SCHEDULER_SERVICE=dask-scheduler
  readonly DASK_CONDA_ENV="/opt/conda/miniconda3/envs/dask-rapids"

  # Knox config
  readonly KNOX_HOME=/usr/lib/knox
  readonly KNOX_DASK_DIR="${KNOX_HOME}/data/services/dask/0.1.0"
  readonly KNOX_DASKWS_DIR="${KNOX_HOME}/data/services/daskws/0.1.0"
  enable_worker_service="0"

  free_mem="$(awk '/^MemFree/ {print $2}' /proc/meminfo)"
  # Write to a ramdisk instead of churning the persistent disk
  if [[ ${free_mem} -ge 5250000 ]]; then
    tmpdir=/mnt/shm
    mkdir -p /mnt/shm
    mount -t tmpfs tmpfs /mnt/shm

    # Download conda packages to tmpfs
    /opt/conda/miniconda3/bin/conda config --add pkgs_dirs /mnt/shm
    mount -t tmpfs tmpfs /mnt/shm

    # Download pip packages to tmpfs
    pip config set global.cache-dir /mnt/shm || echo "unable to set global.cache-dir"

    # Download OS packages to tmpfs
    if is_debuntu ; then
      mount -t tmpfs tmpfs /var/cache/apt/archives
    else
      mount -t tmpfs tmpfs /var/cache/dnf
    fi
  else
    tmpdir=/tmp
  fi
  install_log="${tmpdir}/install.log"
  trap exit_handler EXIT

  # Monitor disk usage in a screen session
  if is_debuntu ; then
      apt-get install -y -qq screen
  else
      dnf -y -q install screen
  fi
  df -h / | tee "${tmpdir}/disk-usage.log"
  touch "${tmpdir}/keep-running-df"
  screen -d -m -US keep-running-df \
    bash -c "while [[ -f ${tmpdir}/keep-running-df ]] ; do df -h / | tee -a ${tmpdir}/disk-usage.log ; sleep 5s ; done"
}

prepare_to_install

main
