#!/bin/bash

# Copyright 2020 Google LLC
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

# This initialization action script will install Dask and other relevant
# libraries on a Dataproc cluster. This is supported for either "yarn" or
# "standalone" runtimes Please see dask.org and yarn.dask.org for more
# information.

set -euxo pipefail

readonly DEFAULT_CONDA_ENV=$(conda info --base)
readonly DASK_YARN_CONFIG_DIR=/etc/dask/
readonly DASK_YARN_CONFIG_FILE=${DASK_YARN_CONFIG_DIR}/config.yaml

readonly DASK_RUNTIME="$(/usr/share/google/get_metadata_value attributes/dask-runtime || echo 'yarn')"
readonly RUN_WORKER_ON_MASTER="$(/usr/share/google/get_metadata_value attributes/dask-worker-on-master || echo 'true')"
readonly DASK_VERSION='2022.1'

readonly ROLE="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"
readonly MASTER="$(/usr/share/google/get_metadata_value attributes/dataproc-master)"

# Dask 'standalone' config
readonly DASK_LAUNCHER=/usr/local/bin/dask-launcher.sh
readonly DASK_SERVICE=dask-cluster
readonly DASK_UI_PORT=8787

readonly KNOX_HOME=/usr/lib/knox
readonly KNOX_DASK_DIR=${KNOX_HOME}/data/services/dask/0.1.0
readonly KNOX_DASKWS_DIR=${KNOX_HOME}/data/services/daskws/0.1.0

CONDA_PACKAGES=("dask=${DASK_VERSION}" 'dask-bigquery' 'dask-ml' 'dask-sql')

if [[ "${DASK_RUNTIME}" == 'yarn' ]]; then
  # Pin `distributed` package version because `dask-yarn` 0.9
  # is not compatible with `distributed` package 2022.2 and newer:
  # https://github.com/dask/dask-yarn/issues/155
  CONDA_PACKAGES+=('dask-yarn=0.9' "distributed=${DASK_VERSION}")
fi
# Downgrade `google-cloud-bigquery` on Dataproc 2.0
# to fix compatibility with old Arrow version
if [[ "${DATAPROC_VERSION}" == '2.0' ]]; then
  CONDA_PACKAGES+=('google-cloud-bigquery=2')
fi
readonly CONDA_PACKAGES

function execute_with_retries() {
  local -r cmd=$1
  for ((i = 0; i < 10; i++)); do
    if eval "$cmd"; then
      return 0
    fi
    sleep 5
  done
  echo "Cmd '${cmd}' failed."
  return 1
}

function configure_dask_yarn() {
  # Minimal custom configuration is required for this
  # setup. Please see https://yarn.dask.org/en/latest/quickstart.html#usage
  # for information on tuning Dask-Yarn environments.
  mkdir -p ${DASK_YARN_CONFIG_DIR}

  cat <<EOF >"${DASK_YARN_CONFIG_FILE}"
# Config file for Dask Yarn.
#
# These values are joined on top of the default config, found at
# https://yarn.dask.org/en/latest/configuration.html#default-configuration

yarn:
  environment: python://${DEFAULT_CONDA_ENV}/bin/python

  worker:
    count: 2
EOF
}

function install_systemd_dask_service() {
  echo "Installing systemd Dask service..."
  local -r dask_worker_local_dir="/tmp/dask"

  mkdir -p "${dask_worker_local_dir}"

  if [[ "${ROLE}" == "Master" ]]; then
    cat <<EOF >"${DASK_LAUNCHER}"
#!/bin/bash
if [[ "${RUN_WORKER_ON_MASTER}" == true ]]; then
  echo "dask-worker starting, logging to /var/log/dask-worker.log."
  ${DEFAULT_CONDA_ENV}/bin/dask-worker ${MASTER}:8786 --local-directory=${dask_worker_local_dir} --memory-limit=auto > /var/log/dask-worker.log 2>&1 &
fi
echo "dask-scheduler starting, logging to /var/log/dask-scheduler.log."
${DEFAULT_CONDA_ENV}/bin/dask-scheduler > /var/log/dask-scheduler.log 2>&1
EOF
  else
    cat <<EOF >"${DASK_LAUNCHER}"
#!/bin/bash
echo "dask-worker starting, logging to /var/log/dask-worker.log."
${DEFAULT_CONDA_ENV}/bin/dask-worker ${MASTER}:8786 --local-directory=${dask_worker_local_dir} --memory-limit=auto > /var/log/dask-worker.log 2>&1
EOF
  fi
  chmod 750 "${DASK_LAUNCHER}"

  local -r dask_service_file=/usr/lib/systemd/system/${DASK_SERVICE}.service
  cat <<EOF >"${dask_service_file}"
[Unit]
Description=Dask Cluster Service
[Service]
Type=simple
Restart=on-failure
ExecStart=/bin/bash -c 'exec ${DASK_LAUNCHER}'
[Install]
WantedBy=multi-user.target
EOF
  chmod a+r "${dask_service_file}"

  systemctl daemon-reload
  systemctl enable "${DASK_SERVICE}"
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

  sed -i \
    "/<\/topology>/i <service><role>DASK<\/role><url>http://localhost:${DASK_UI_PORT}<\/url><\/service> <service><role>DASKWS<\/role><url>ws:\/\/localhost:${DASK_UI_PORT}<\/url><\/service>" \
    /etc/knox/conf/topologies/default.xml

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

  restart_knox
}


function main() {
  # Install conda packages
  execute_with_retries "mamba install -y ${CONDA_PACKAGES[*]}"

  if [[ "${DASK_RUNTIME}" == "yarn" ]]; then
    # Create Dask YARN config file
    configure_dask_yarn
  elif [[ "${DASK_RUNTIME}" == "standalone" ]]; then
    # Create Dask service
    install_systemd_dask_service

    echo "Starting Dask 'standalone' cluster..."
    systemctl start "${DASK_SERVICE}"

    configure_knox_for_dask
  else
    echo "Unsupported Dask Runtime: ${DASK_RUNTIME}"
    exit 1
  fi

  echo "Dask for ${DASK_RUNTIME} successfully initialized."
}


main
