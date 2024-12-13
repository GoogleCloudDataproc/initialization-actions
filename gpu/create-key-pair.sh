#!/bin/bash
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
#
# This script creates a key pair and publishes to cloud secrets or
# fetches an already published key pair from cloud secrets

set -e

# https://github.com/glevand/secure-boot-utils

# https://cloud.google.com/compute/shielded-vm/docs/creating-shielded-images#adding-shielded-image

# https://cloud.google.com/compute/shielded-vm/docs/creating-shielded-images#generating-security-keys-certificates

# https://wiki.archlinux.org/title/Unified_Extensible_Firmware_Interface/Secure_Boot#Creating_keys

ITERATION=042

CURRENT_PROJECT_ID="$(gcloud config get project)"
if [[ -z "${CURRENT_PROJECT_ID}" ]]; then
    echo 'project is not set.  please set with `gcloud config set project ${PROJECT_ID}`' >&2
    exit -1
fi
PROJECT_ID="${CURRENT_PROJECT_ID}"

function create_key () {
    local EFI_VAR_NAME="$1"
    local CN_VAL="$2"
    local PRIVATE_KEY="tls/${EFI_VAR_NAME}.rsa"
    local CACERT="tls/${EFI_VAR_NAME}.pem"
    local CACERT_DER="tls/${EFI_VAR_NAME}.der"
    CA_KEY_SECRET_NAME="efi-${EFI_VAR_NAME}-priv-key-${ITERATION}"
    CA_CERT_SECRET_NAME="efi-${EFI_VAR_NAME}-pub-key-${ITERATION}"
    # If the secrets exist in secret manager, populate the tls/ directory
    if [[ ! -f "${PRIVATE_KEY}" ]] && gcloud secrets describe "${CA_CERT_SECRET_NAME}" > /dev/null ; then
      mkdir -p tls

      gcloud secrets versions access "1" \
        --project="${PROJECT_ID}" \
        --secret="${CA_KEY_SECRET_NAME}" \
        | dd of="${PRIVATE_KEY}" status=none

      gcloud secrets versions access "1" \
        --project="${PROJECT_ID}" \
        --secret="${CA_CERT_SECRET_NAME}" \
        | base64 --decode \
        | dd of="${CACERT_DER}" status=none

      # Create a PEM-format version of the cert
      openssl x509 \
        -inform DER \
        -in "${CACERT_DER}" \
        -outform PEM \
        -out "${CACERT}"

      MS_UEFI_CA="tls/MicCorUEFCA2011_2011-06-27.crt"
      curl -s -L -o "${MS_UEFI_CA}" 'https://go.microsoft.com/fwlink/p/?linkid=321194'

      echo "${CA_KEY_SECRET_NAME}" > tls/private-key-secret-name.txt
      echo "${CA_CERT_SECRET_NAME}" > tls/public-key-secret-name.txt
      modulus_md5sum="$(openssl rsa -noout -modulus -in ${PRIVATE_KEY} | openssl md5 | awk '{print $2}' | tee tls/modulus-md5sum.txt)"
      return
    fi

    if [[ -f "${PRIVATE_KEY}" ]]; then
        modulus_md5sum="$(cat tls/modulus-md5sum.txt)"
        return
    fi
    mkdir -p tls

    echo "generating '${CN_VAL}' '${CACERT}', '${CACERT_DER}' and '${PRIVATE_KEY}'" >&2
    # Generate new x.509 key and cert
    openssl req \
            -newkey rsa:3072 \
            -nodes \
            -keyout "${PRIVATE_KEY}" \
            -new \
            -x509 \
            -sha256 \
            -days 3650 \
            -subj "/CN=${CN_VAL}/" \
            -out "${CACERT}"

    # Create a DER-format version of the cert
    openssl x509 \
            -outform DER \
            -in "${CACERT}" \
            -outform DER \
            -in "${CACERT}" \
            -out "${CACERT_DER}"

    # Create a new secret containing private key
    gcloud secrets create "${CA_KEY_SECRET_NAME}" \
           --project="${PROJECT_ID}" \
           --replication-policy="automatic" \
           --data-file="${PRIVATE_KEY}"

    echo "Private key secret name: '${CA_KEY_SECRET_NAME}'" >&2
    echo "${CA_KEY_SECRET_NAME}" > tls/private-key-secret-name.txt

    # Create a new secret containing public key
    cat "${CACERT_DER}" | base64 > "${CACERT_DER}.base64"
    gcloud secrets create "${CA_CERT_SECRET_NAME}" \
           --project="${PROJECT_ID}" \
           --replication-policy="automatic" \
           --data-file="${CACERT_DER}.base64"

    modulus_md5sum="$(openssl x509 -noout -modulus -in ${CACERT} | openssl md5 | awk '{print $2}')"
    echo "modulus-md5sum: ${modulus_md5sum}" >&2
    echo "${modulus_md5sum}" > tls/modulus-md5sum.txt
    echo "Public key secret name: '${CA_CERT_SECRET_NAME}'" >&2
    echo "${CA_CERT_SECRET_NAME}" > tls/public-key-secret-name.txt

}

EFI_VAR_NAME=db

create_key "${EFI_VAR_NAME}" "Cloud Dataproc Custom Image CA ${ITERATION}"

echo "modulus_md5sum=${modulus_md5sum}"
echo "private_secret_name=${CA_KEY_SECRET_NAME}"
echo "public_secret_name=${CA_CERT_SECRET_NAME}"
echo "secret_project=${PROJECT_ID}"
echo "secret_version=1"
