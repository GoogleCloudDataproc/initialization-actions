#!/bin/bash

set -euxo pipefail

readonly DEFAULT_CUDA_VERSION='10-0'
readonly CUDA_VERSION=$(/usr/share/google/get_metadata_value attributes/cuda-version ||
  echo -n "${DEFAULT_CUDA_VERSION}")

readonly DEFAULT_NCCL_URL='https://developer.download.nvidia.com/compute/machine-learning/repos/ubuntu1804/x86_64/nvidia-machine-learning-repo-ubuntu1804_1.0.0-1_amd64.deb'
readonly NCCL_URL=$(/usr/share/google/get_metadata_value attributes/nccl-url ||
  echo -n "${DEFAULT_NCCL_URL}")

readonly DEFAULT_NCCL_VERSION='2.4.8'
readonly NCCL_VERSION=$(/usr/share/google/get_metadata_value attributes/nccl-version ||
  echo -n "${DEFAULT_NCCL_VERSION}")

apt-get update
apt-get install build-essential

wget https://developer.download.nvidia.com/compute/cuda/repos/ubuntu1804/x86_64/cuda-ubuntu1804.pin
mv cuda-ubuntu1804.pin /etc/apt/preferences.d/cuda-repository-pin-600
apt-key adv --fetch-keys https://developer.download.nvidia.com/compute/cuda/repos/ubuntu1804/x86_64/7fa2af80.pub
add-apt-repository "deb http://developer.download.nvidia.com/compute/cuda/repos/ubuntu1804/x86_64/ /"
apt-get update

if [[ "${CUDA_VERSION}" != '10-0' ]]; then
  apt-get -y install cuda
else
  apt-get -y install cuda-10-0
fi

wget --progress=dot:mega -O nccl.deb "${NCCL_URL}"
dpkg -i nccl.deb
apt update
apt install "libnccl2=${NCCL_VERSION}-1+cuda${CUDA_VERSION//\-/\.}" "libnccl-dev=${NCCL_VERSION}-1+cuda${CUDA_VERSION//\-/\.}" -y 

/usr/bin/nvidia-smi -c EXCLUSIVE_PROCESS



