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

# Downloads and installs all appropriate hadoop packages as user 'hadoop'.
# Also adds installation specific configs into the login scripts of user
# 'hadoop'.

set -e

# Install Snappy native libs first:
install_application "libsnappy1" "snappy"
install_application "libsnappy-dev" "snappy-devel"

# We'll use this to get architecture information later
install_application "file"

INSTALL_TMP_DIR=${INSTALL_TMP_DIR}/hadoop-$(date +%s)
mkdir -p ${INSTALL_TMP_DIR}

HADOOP_TARBALL=${HADOOP_TARBALL_URI##*/}
HADOOP_TARBALL_URI_SCHEME=${HADOOP_TARBALL_URI%%://*}

download_bd_resource "${HADOOP_TARBALL_URI}" \
    "${INSTALL_TMP_DIR}/${HADOOP_TARBALL}"

tar -C ${INSTALL_TMP_DIR} -xvzf ${INSTALL_TMP_DIR}/${HADOOP_TARBALL}
mkdir -p $(dirname ${HADOOP_INSTALL_DIR})
mv ${INSTALL_TMP_DIR}/hadoop*/ ${HADOOP_INSTALL_DIR}

# Hadoop 2 tarballs only come with 32-bit binaries that cause the JVM to
# complain.
find ${HADOOP_INSTALL_DIR}/lib -follow -xtype f \
    | xargs -r file -Le elf | grep '32-bit' | cut -d':' -f1 | xargs -r rm -f

# Update login scripts
add_to_path_at_login "${HADOOP_INSTALL_DIR}/bin"
HADOOP_CONFIG_SCRIPT="${HADOOP_INSTALL_DIR}/libexec/hadoop-config.sh"
cat << EOF | add_to_login_scripts
# Try running hadoop-config.sh to see if finishes cleanly, because it calls
# 'exit 1', when it encounters problems.
if [ -r ${HADOOP_CONFIG_SCRIPT} ] \\
    && bash "${HADOOP_CONFIG_SCRIPT}"; then
    . "${HADOOP_CONFIG_SCRIPT}"
fi
EOF

export JAVA_HOME=$(readlink -f $(which java) | sed 's|/bin/java$||')
export HADOOP_BIN_DIR="${HADOOP_INSTALL_DIR}/bin"
echo "Running hadoop version..."
$HADOOP_BIN_DIR/hadoop version
HADOOP_VERSION="$($HADOOP_BIN_DIR/hadoop version \
    | head -n1 | sed 's/ /_/')"
export HADOOP_CLASSPATH="$($HADOOP_BIN_DIR/hadoop classpath)"
PLATFORM_NAME_CLASS="org.apache.hadoop.util.PlatformName"
echo "Running platform util..."
JAVA_PLATFORM="$(CLASSPATH=$HADOOP_CLASSPATH java $PLATFORM_NAME_CLASS)"
echo "Platform detected as ${JAVA_PLATFORM}"

# Attempt to determine which if any platform libraries to download
if [ -z "${HADOOP_NATIVE_LIBRARIES_TARBALL_URI}" ]; then
  # A library tarball hasn't been explicitly set, try to find one to use:
  readonly native_uri_root="gs://hadoop-native-dist"
  NATIVE_URI="${native_uri_root}/${HADOOP_VERSION}-${JAVA_PLATFORM}.tar.gz"
  echo "Constructed \$NATIVE_URI: ${NATIVE_URI}"
  if gsutil stat "${NATIVE_URI}";  then
    echo "Native libraries available for download."
    HADOOP_NATIVE_LIBRARIES_TARBALL_URI="${NATIVE_URI}"
  else
    echo "Native libraries NOT available for download."
  fi
fi

if [ ! -z "${HADOOP_NATIVE_LIBRARIES_TARBALL_URI}" ]; then
  # We should have a usable URI. Try to unpack and setup native libs:

  download_bd_resource "${HADOOP_NATIVE_LIBRARIES_TARBALL_URI}" \
      "${HADOOP_INSTALL_DIR}"
  NATIVE_TARBALL="${HADOOP_NATIVE_LIBRARIES_TARBALL_URI##*/}"
  tar -C ${HADOOP_INSTALL_DIR} -xvf "${HADOOP_INSTALL_DIR}/${NATIVE_TARBALL}"
fi

# Even if we didn't unpack a native tarball, we can still add snappy
# links to the lib directory if not already present:

# Create symlinks from /usr/lib and /usr/lib64 to the target
# directory. Assumes no 32 bit libraries are present in either
# lib directory.
function install_snappy_links_to() {
  local target="$1"
  echo "Checking for snappy in ${target}..."
  if [ ! -e "${target}/libsnappy.so" ]; then
    echo "Existing libsnappy.so NOT found."
    echo "Installing snappy links to ${target}..."

    for libdir in /usr/lib /usr/lib64; do
      if [[ -d ${libdir} ]]; then
        find ${libdir} -maxdepth 1 -name 'libsnappy*' \
            | xargs -r ln -s -t "${target}"
      fi
    done
  else
    echo "Existing libsnappy.so detected. Links will NOT be installed."
  fi
}

# Add links for Snappy (it's easiest if everything Snappy is located
# in the same native dir. LZO and gzip don't suffer the same discovery
# process). Hadoop 1 wants these in a platform specific directory while
# Hadoop 2 wants things in lib/native:

PLATFORM_SPECIFIC_DIR="${HADOOP_INSTALL_DIR}/lib/native/${JAVA_PLATFORM}"

if [ -d "${PLATFORM_SPECIFIC_DIR}" ]; then
    install_snappy_links_to "${PLATFORM_SPECIFIC_DIR}"
else
    install_snappy_links_to "${HADOOP_INSTALL_DIR}/lib/native"
fi

chown -R hadoop:hadoop  ${HADOOP_INSTALL_DIR}
