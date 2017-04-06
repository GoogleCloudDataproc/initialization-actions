#!/bin/bash
set -e

# 0.1 Ensure we have conda installed and available on the PATH
if [[ ! -v CONDA_BIN_PATH ]]; then
    source /etc/profile.d/conda.sh
fi

echo "echo \$USER: $USER"
echo "echo \$PWD: $PWD"
echo "echo \$PATH: $PATH"
echo "echo \$CONDA_BIN_PATH: $CONDA_BIN_PATH"

# 0.2. Specify conda environment name (recommend leaving as root)
if [[ ! -v CONDA_ENV_NAME ]]; then
    echo "No conda environment name specified, setting to 'root' env..."
    CONDA_ENV_NAME='root'
# Force conda env name to be set to root for now, until a braver soul manages the complexity of environment activation
# across the cluster.
else
    echo "conda environment name is set to $CONDA_ENV_NAME"
    if [[ ! $CONDA_ENV_NAME == 'root' ]]
        then
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
    if [[ $CONDA_ENV_NAME == 'root' ]]
        then
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
if conda info --envs | grep -q $CONDA_ENV_NAME
    then
    echo "conda environment $CONDA_ENV_NAME detected, skipping env creation..."
else
    echo "Creating conda environment directly..."
    conda create --quiet --yes --name=$CONDA_ENV_NAME python || true
    echo "conda environment $CONDA_ENV_NAME created..."
fi
if [[ ! $CONDA_ENV_NAME == 'root' ]]
    then
    echo "Activating $CONDA_ENV_NAME environment..."
    source activate $CONDA_ENV_NAME
fi

# 3. Install conda and pip packages (if specified)
if [[ -v CONDA_PACKAGES ]]; then
    echo "Installing conda packages for $CONDA_ENV_NAME..."
    echo "conda packages requested: $CONDA_PACKAGES"
    conda install $CONDA_PACKAGES
fi
if [[ -v PIP_PACKAGES ]]; then
    echo "Installing pip packages for $CONDA_ENV_NAME..."
    echo "conda packages requested: $PIP_PACKAGES"
    pip install $PIP_PACKAGES
fi

# 2. Append profiles with conda env source activate
echo "Attempting to append /etc/profile.d/conda.sh to activate conda env at login..."
if [[ -f "/etc/profile.d/conda.sh"  ]]  && [[ ! $CONDA_ENV_NAME == 'root' ]]
    then
    if grep -ir "source activate $CONDA_ENV_NAME" /etc/profile.d/conda.sh
        then
        echo "conda env activation found in /etc/profile.d/conda.sh, skipping..."
    else
        echo "Appending /etc/profile.d/conda.sh to activate conda env $CONDA_ENV_NAME for shell..."
        sudo echo "source activate $CONDA_ENV_NAME" | tee -a /etc/profile.d/conda.sh
        echo "./etc/profile.d/conda.sh successfully appended!"
    fi
elif [[ $CONDA_ENV_NAME == 'root' ]]
    then
    echo "The conda env specified is 'root', the default environment, no need to activate, skipping..."
else
    echo "No file detected at /etc/profile.d/conda.sh..."
    echo "Are you sure you installed conda via bootstrap-conda.sh?"
    exit 1
fi
