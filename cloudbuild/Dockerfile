# This Dockerfile builds the container from which presubmit tests are run
# Cloud Build orchestrates this process.

FROM gcr.io/cloud-builders/gcloud

RUN useradd -m -d /home/ia-tests -s /bin/bash ia-tests

COPY --chown=ia-tests:ia-tests . /init-actions

# Install Bazel:
# https://docs.bazel.build/versions/master/install-ubuntu.html
ENV bazel_kr_path=/usr/share/keyrings/bazel-keyring.gpg \
    bazel_version=7.4.0 \
    bazel_repo_data="http://storage.googleapis.com/bazel-apt stable jdk1.8" \
    bazel_repo_file="/etc/apt/sources.list.d/bazel.list" \
    DEBIAN_FRONTEND=noninteractive
RUN apt-get install -y -qq curl >/dev/null 2>&1 && \
    apt-get clean
RUN /usr/bin/curl -s https://bazel.build/bazel-release.pub.gpg | \
    gpg --import --no-default-keyring --keyring "${bazel_kr_path}" && \
    echo "deb [arch=amd64 signed-by=${bazel_kr_path}] ${bazel_repo_data}" | \
    dd of="${bazel_repo_file}" status=none && \
    apt-get update -qq
RUN apt-get autoremove -y -qq > /dev/null 2>&1 && \
    apt-get install -y -qq default-jdk python3-setuptools bazel-${bazel_version} > /dev/null 2>&1 && \
    apt-get clean

# Set bazel-${bazel_version} as the default bazel alternative in this container
RUN update-alternatives --install /usr/bin/bazel bazel /usr/bin/bazel-${bazel_version} 1 && \
    update-alternatives                    --set bazel /usr/bin/bazel-${bazel_version}

USER ia-tests
