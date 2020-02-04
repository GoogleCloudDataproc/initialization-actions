#!/bin/bash

set -ux

readonly test_type="$1"
readonly KNOX_GW_CONFIG_GCS=$(/usr/share/google/get_metadata_value attributes/knox-gw-config)
readonly MASTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-master)

function test_installation() {
  topology="$1"

  sudo -u knox /usr/lib/knox/bin/knoxcli.sh export-cert --type PEM

  # We replace the address of the WebHDFS from localhost to the hostname
  # since HA clusters bind the namenode to the ip address.
  sudo sed -i "s/localhost/$(hostname -A | tr -d '[:space:]')/g" "/etc/knox/conf/topologies/${topology}.xml"

  if [[ "$test_type" == "localhost" ]]; then
    host_address="localhost"
  elif [[ "$test_type" == "hostname" ]]; then
    host_address="$(hostname -A | tr -d '[:space:]')"
  else
    echo "we are testing an undefined case"
    return 1
  fi

  # try multiple times till knox is up and working
  for _ in {1..10}; do
    sleep 15s
    if curl -i --cacert /usr/lib/knox/data/security/keystores/gateway-identity.pem -u guest:guest-password \
      -X GET "https://${host_address}:8443/gateway/${topology}/webhdfs/v1/?op=LISTSTATUS" |
      tee test_output |
      grep "200 OK"; then
      return 0
    fi
    cat test_output
  done

  return 1

}

# to test update, we will upload a new topology to gs bucket, and check whether it appears
# we assume that knox initialization action is the very first one, /etc/google-dataproc/startup-scripts/dataproc-initialization-script-0
function test_update_new_topology() {
  gsutil cp /etc/knox/conf/topologies/example-hive-pii.xml "${KNOX_GW_CONFIG_GCS}/topologies/update_topology.xml"
  sudo /bin/bash /etc/google-dataproc/startup-scripts/dataproc-initialization-script-0 update
  test_installation update_topology
  [[ $? == 1 ]] && return 1
  return 0
}

if [[ "${MASTER_NAME}" == $(hostname) ]]; then
  test_installation example-hive-pii
  [[ $? == 1 ]] && exit 1
  test_update_new_topology
  [[ $? == 1 ]] && exit 1
  exit 0
else
  sudo systemctl status knox
  [[ $? == 0 ]] && exit 1 # knox is only installed into the main master. It is not a must though.
  exit 0
fi
