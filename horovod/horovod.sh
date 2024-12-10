#!/bin/bash

# Copyright 2021 Google LLC
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
#
# This initialization action will install Horovod for TensorFlow, PyTorch,
# MXNet, and Spark. For more information, refer to the Horovod repo
# on Github: https://github.com/horovod/horovod.

set -euxo pipefail

readonly DEFAULT_HOROVOD_VERSION="0.21.2"
readonly DEFAULT_TENSORFLOW_VERSION="2.4.1"
readonly DEFAULT_PYTORCH_VERSION="1.7.1"
readonly DEFAULT_TORCHVISION_VERSION="0.8.2"
readonly DEFAULT_MXNET_VERSION="1.7.0.post1"
readonly DEFAULT_CUDA_VERSION="12.4"

HOROVOD_VERSION="$(/usr/share/google/get_metadata_value attributes/horovod-version || echo ${DEFAULT_HOROVOD_VERSION})"
readonly HOROVOD_VERSION
TENSORFLOW_VERSION="$(/usr/share/google/get_metadata_value attributes/tensorflow-version || echo ${DEFAULT_TENSORFLOW_VERSION})"
readonly TENSORFLOW_VERSION
PYTORCH_VERSION="$(/usr/share/google/get_metadata_value attributes/pytorch-version || echo ${DEFAULT_PYTORCH_VERSION})"
readonly PYTORCH_VERSION
TORCHVISION_VERSION="$(/usr/share/google/get_metadata_value attributes/torvision-version || echo ${DEFAULT_TORCHVISION_VERSION})"
readonly TORCHVISION_VERSION
MXNET_VERSION="$(/usr/share/google/get_metadata_value attributes/mxnet-version || echo ${DEFAULT_MXNET_VERSION})"
readonly MXNET_VERSION

CUDA_VERSION="$(/usr/share/google/get_metadata_value attributes/cuda-version || echo ${DEFAULT_CUDA_VERSION})"
readonly CUDA_VERSION

HOROVOD_ENV_VARS="$(/usr/share/google/get_metadata_value attributes/horovod-env-vars || echo "")"
readonly HOROVOD_ENV_VARS

INSTALL_MPI="$(/usr/share/google/get_metadata_value attributes/install-mpi || echo 'false')"
readonly INSTALL_MPI

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

function install_mpi() {
  local -r mpi_version="4.1.0"
  local -r mpi_url="https://download.open-mpi.org/release/open-mpi/v${mpi_version%.*}/openmpi-${mpi_version}.tar.gz"
  local tmp_dir
  tmp_dir=$(mktemp -d -t mlvm-horovod-mpi-XXXX)

  wget -nv --timeout=30 --tries=5 --retry-connrefused -P "${tmp_dir}" "${mpi_url}"
  gunzip -c "${tmp_dir}/openmpi-${mpi_version}.tar.gz" | tar xf - -C "${tmp_dir}"

  pushd "${tmp_dir}/openmpi-${mpi_version}"
  ./configure --prefix=/usr/local --enable-mpirun-prefix-by-default
  make all install
  ldconfig
  popd
}

function configure_hadoop_env() {
  # Horovod relies on CLASSPATH being set in the environment. This needs to
  # contain the Hadoop jars.
  cat <<'EOF' >>/etc/profile.d/horovod.sh
export CLASSPATH=$(hadoop classpath --glob)
EOF
}

function install_frameworks() {
  local framework_packages=()

  # Add gpu-versions of libraries
  if (lspci | grep -q NVIDIA); then
    local torch_packages=(
      "torch==${PYTORCH_VERSION}"
      "torchvision==${TORCHVISION_VERSION}"
    )
    pip install "${torch_packages[@]}" -f "https://download.pytorch.org/whl/torch_stable.html"
    if [[ ${TENSORFLOW_VERSION} == "1."* ]]; then
      framework_packages+=("tensorflow-gpu==${TENSORFLOW_VERSION}")
    else
      framework_packages+=("tensorflow==${TENSORFLOW_VERSION}")
    fi
  else
    framework_packages+=(
      "torch==${PYTORCH_VERSION}"
      "torchvision==${TORCHVISION_VERSION}"
      "tensorflow==${TENSORFLOW_VERSION}"
      "mxnet==${MXNET_VERSION}"
    )
  fi

  pip install "${framework_packages[@]}"
}

function install_horovod() {
  # Install cmake
  execute_with_retries "apt-get install -y cmake"

  # Install Horovod
  local horovod_build_frameworks="tensorflow,pytorch,spark"

  # Horovod on Dataproc with MXNet only supported with CPUs
  if ! (lspci | grep -q NVIDIA); then
    horovod_build_frameworks+=",mxnet"
  fi

  eval "${HOROVOD_ENV_VARS} pip install --no-cache-dir horovod[${horovod_build_frameworks}]==${HOROVOD_VERSION}"
}

function main() {
  # Optionally install MPI pip un
  if [[ "${INSTALL_MPI}" == 'true' ]]; then
    install_mpi
  fi

  # Update environment variables
  configure_hadoop_env

  # Install Frameworks
  install_frameworks

  # Install Horovod
  install_horovod
}

main
