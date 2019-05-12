#!/bin/bash

apt-get update
apt-get install -y pciutils
export DEBIAN_FRONTEND=noninteractive
apt-get install -y "linux-headers-$(uname -r)"

readonly GPU_DRIVER_URL=$(/usr/share/google/get_metadata_value attributes/gpu-driver-url)
readonly GPU_DRIVER=$(/usr/share/google/get_metadata_value attributes/gpu-driver)

wget ${GPU_DRIVER_URL}
chmod a+x "./${GPU_DRIVER}"
"./${GPU_DRIVER}" --silent
