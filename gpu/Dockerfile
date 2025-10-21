# This Dockerfile builds the container from which manual tests are run
# This process needs to be executed manually from a git clone
#
# See manual-test-runner.sh for instructions

FROM gcr.io/cloud-builders/gcloud

RUN useradd -m -d /home/ia-tests -s /bin/bash ia-tests

# Installed here are packages on which the tests depend
RUN apt-get -qq update \
  && apt-get -y -qq install \
     apt-transport-https apt-utils \
     ca-certificates libmime-base64-perl gnupg \
     curl jq less screen > /dev/null 2>&1  && apt-get clean

# Install bazel signing key, repo and package
ENV bazel_kr_path=/usr/share/keyrings/bazel-keyring.gpg \
    bazel_version=7.4.0 \
    bazel_repo_data="http://storage.googleapis.com/bazel-apt stable jdk1.8" \
    DEBIAN_FRONTEND=noninteractive

RUN /usr/bin/curl -s https://bazel.build/bazel-release.pub.gpg \
      | gpg --dearmor -o "${bazel_kr_path}" \
    && echo "deb [arch=amd64 signed-by=${bazel_kr_path}] ${bazel_repo_data}" \
      | dd of=/etc/apt/sources.list.d/bazel.list status=none \
    && apt-get update -qq

RUN apt-get autoremove -y -qq > /dev/null 2>&1 && \
    apt-get install -y -qq default-jdk python3-setuptools bazel-${bazel_version} > /dev/null 2>&1 && \
    apt-get clean

# Set bazel-${bazel_version} as the default bazel alternative in this container
RUN update-alternatives --install /usr/bin/bazel bazel /usr/bin/bazel-${bazel_version} 1 && \
    update-alternatives --set bazel /usr/bin/bazel-${bazel_version}

# Install here any utilities you find useful when troubleshooting
RUN apt-get -y -qq install emacs-nox vim uuid-runtime > /dev/null 2>&1 && apt-get clean

WORKDIR /init-actions

USER ia-tests
COPY --chown=ia-tests:ia-tests "cloudbuild/key.json" /key.json
COPY --chown=ia-tests:ia-tests . ${WORKDIR}

ENTRYPOINT ["/bin/bash"]
#CMD ["/bin/bash"]
