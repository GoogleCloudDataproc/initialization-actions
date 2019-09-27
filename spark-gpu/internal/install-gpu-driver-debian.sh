#!/bin/bash

set -euxo pipefail

readonly DEFAULT_GPU_DRIVER_URL='http://us.download.nvidia.com/tesla/418.87/NVIDIA-Linux-x86_64-418.87.00.run'
readonly GPU_DRIVER_URL=$(/usr/share/google/get_metadata_value attributes/gpu-driver-url ||
  echo -n "${DEFAULT_GPU_DRIVER_URL}")

readonly DEFAULT_CUDA_URL='https://developer.nvidia.com/compute/cuda/10.0/Prod/local_installers/cuda_10.0.130_410.48_linux'
readonly CUDA_URL=$(/usr/share/google/get_metadata_value attributes/gpu-cuda-url ||
  echo -n "${DEFAULT_CUDA_URL}")

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
DEBIAN_FRONTEND=noninteractive apt-get install -y pciutils "linux-headers-$(uname -r)"

wget --progress=dot:mega -O driver.run "${GPU_DRIVER_URL}"
chmod +x "./driver.run"
"./driver.run" --silent

wget --progress=dot:mega -O cuda.run "${CUDA_URL}"
chmod +x "./cuda.run"
"./cuda.run" --silent --toolkit --no-opengl-libs

wget --progress=dot:mega -O nccl.deb "${GPU_NCCL_URL}"
chmod +x "./nccl.deb"
dpkg -i nccl.deb
apt update
apt install "libnccl2=${NCCL_VERSION}-1+cuda${CUDA_VERSION//\-/\.}" "libnccl-dev=${NCCL_VERSION}-1+cuda${CUDA_VERSION//\-/\.}" -y 

/usr/bin/nvidia-smi -c EXCLUSIVE_PROCESS
