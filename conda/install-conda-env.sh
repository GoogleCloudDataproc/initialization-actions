#!/bin/bash
set -exo pipefail

readonly NOT_SUPPORTED_MESSAGE="Conda initialization action is not supported on Dataproc 2.0+.
Use Anaconda Component instead: https://cloud.google.com/dataproc/docs/concepts/components/anaconda"
[[ $DATAPROC_VERSION = 2.* ]] && echo "$NOT_SUPPORTED_MESSAGE" && exit 1

if [[ -f /etc/profile.d/effective-python.sh ]]; then
  PROFILE_SCRIPT_PATH=/etc/profile.d/effective-python.sh
elif [[ -f /etc/profile.d/conda.sh ]]; then
  PROFILE_SCRIPT_PATH=/etc/profile.d/conda.sh
fi

# 0.1 Ensure we have conda installed and available on the PATH
if [[ -f "${PROFILE_SCRIPT_PATH}" ]]; then
  source "${PROFILE_SCRIPT_PATH}"
fi

echo "echo \$USER: $USER"
echo "echo \$PWD: $PWD"
echo "echo \$PATH: $PATH"
echo "echo \$CONDA_PATH: $(which conda)"

if ! command -v conda >/dev/null; then
  echo "Conda was not installed."
  exit 1
fi

[ -z "${CONDA_PACKAGES}" ] && CONDA_PACKAGES=$(/usr/share/google/get_metadata_value attributes/CONDA_PACKAGES || true)
[ -z "${PIP_PACKAGES}" ] && PIP_PACKAGES=$(/usr/share/google/get_metadata_value attributes/PIP_PACKAGES || true)

# 0.2. Specify conda environment name (recommend leaving as root)
if [[ ! -v CONDA_ENV_NAME ]]; then
  echo "No conda environment name specified, setting to 'root' env..."
  CONDA_ENV_NAME='root'
# Force conda env name to be set to root for now, until a braver soul manages the complexity of environment activation
# across the cluster.
else
  echo "conda environment name is set to $CONDA_ENV_NAME"
  if [[ ! $CONDA_ENV_NAME == 'root' ]]; then
    echo "Custom conda environment names not supported at this time."
    echo "Force setting conda env to 'root'..."
  fi
  CONDA_ENV_NAME='root'
fi

#conda update --all
# 1. Create conda environment
# 1.1 Update conda env from conda environment.yml (if specified)
# For Dataproc provisioning, we should install to root conda env.
if [[ -v CONDA_ENV_YAML ]]; then
  #CONDA_ENV_NAME=$(grep 'name: ' $CONDA_ENV_YAML | awk '{print $2}')
  # if conda environment name is root, we *update* the root environment with env yaml
  if [[ $CONDA_ENV_NAME == 'root' ]]; then
    echo "Updating root environment with file $CONDA_ENV_YAML"
    conda env update --name=$CONDA_ENV_NAME --file=$CONDA_ENV_YAML
    echo "Root environment updated..."
  # otherwise, perform a typical environment creation via install
  else
    echo "Creating $CONDA_ENV_NAME environment with file $CONDA_ENV_YAML"
    conda env create --name=$CONDA_ENV_NAME --file=$CONDA_ENV_YAML
    echo "conda environment $CONDA_ENV_NAME created..."
  fi
fi

# 1. Or create conda env manually.
echo "Attempting to create conda environment: $CONDA_ENV_NAME"
if conda info --envs | grep -q $CONDA_ENV_NAME; then
  echo "conda environment $CONDA_ENV_NAME detected, skipping env creation..."
else
  echo "Creating conda environment directly..."
  conda create --quiet --yes --name=$CONDA_ENV_NAME python || true
  echo "conda environment $CONDA_ENV_NAME created..."
fi
if [[ ! $CONDA_ENV_NAME == 'root' ]]; then
  echo "Activating $CONDA_ENV_NAME environment..."
  source activate $CONDA_ENV_NAME
fi

# Pin base conda and Python versions to minor version to prevent unexpected upgrades
# while installing conda and pip packages
if conda info --base; then
  CONDA_BASE_PATH=$(conda info --base)
else
  # Older versions of conda don't support the --base flag
  CONDA_BASE_PATH=$(conda info | grep 'default environment' | sed -E 's:\s+default environment\s+\:\s+(.*):\1:g')
fi
CONDA_PINNED_FILE="${CONDA_BASE_PATH}/conda-meta/pinned"
function pin_component_version() {
  local component=$1

  version=$(conda list "${component}" |
    grep -E "^${component}\s+" | sed -E "s/[ ]+/ /g" |
    cut -f2 -d' ' | cut -f1,2 -d'.')
  echo "${component} ${version}.*" >>"${CONDA_PINNED_FILE}"
}
pin_component_version conda
pin_component_version python

# 3. Install conda and pip packages (if specified)
if [[ ! -z "${CONDA_PACKAGES}" ]]; then
  echo "Installing conda packages for $CONDA_ENV_NAME..."
  echo "conda packages requested: $CONDA_PACKAGES"
  conda install $CONDA_PACKAGES
fi
if [[ ! -z "${PIP_PACKAGES}" ]]; then
  echo "Installing pip packages for $CONDA_ENV_NAME..."
  echo "conda packages requested: $PIP_PACKAGES"
  pip install $PIP_PACKAGES
fi

# 2. Append profiles with conda env source activate
echo "Attempting to append ${PROFILE_SCRIPT_PATH} to activate conda env at login..."
if [[ -f "${PROFILE_SCRIPT_PATH}" ]] && [[ ! $CONDA_ENV_NAME == 'root' ]]; then
  if grep -ir "source activate $CONDA_ENV_NAME" "${PROFILE_SCRIPT_PATH}"; then
    echo "conda env activation found in ${PROFILE_SCRIPT_PATH}, skipping..."
  else
    echo "Appending ${PROFILE_SCRIPT_PATH} to activate conda env $CONDA_ENV_NAME for shell..."
    sudo echo "source activate $CONDA_ENV_NAME" | tee -a "${PROFILE_SCRIPT_PATH}"
    echo "${PROFILE_SCRIPT_PATH} successfully appended!"
  fi
elif [[ $CONDA_ENV_NAME == 'root' ]]; then
  echo "The conda env specified is 'root', the default environment, no need to activate, skipping..."
else
  echo "No file detected at ${PROFILE_SCRIPT_PATH}..."
  echo "Are you sure you installed conda?"
  exit 1
fi
