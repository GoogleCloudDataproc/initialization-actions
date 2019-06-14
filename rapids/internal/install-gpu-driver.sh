#!/bin/bash

set -euxo pipefail

readonly DEFAULT_GPU_DRIVER_URL='http://us.download.nvidia.com/tesla/410.104/NVIDIA-Linux-x86_64-410.104.run'
readonly GPU_DRIVER_URL=$(/usr/share/google/get_metadata_value attributes/gpu-driver-url ||
  echo -n "${DEFAULT_GPU_DRIVER_URL}")

apt-get update
DEBIAN_FRONTEND=noninteractive apt-get install -y pciutils "linux-headers-$(uname -r)"

wget --progress=dot:mega -O driver.run "${GPU_DRIVER_URL}"
chmod +x "./driver.run"

"./driver.run" --silent
