#!/bin/bash

apt-get update
apt-get install -y pciutils
export DEBIAN_FRONTEND=noninteractive
apt-get install -y "linux-headers-$(uname -r)"

readonly GPU_DRIVER_URL=$(/usr/share/google/get_metadata_value attributes/gpu-driver-url || echo -n 'http://us.download.nvidia.com/tesla/410.104/NVIDIA-Linux-x86_64-410.104.run')

wget -O driver.run ${GPU_DRIVER_URL}
chmod +x "./driver.run"
"./driver.run" --silent
