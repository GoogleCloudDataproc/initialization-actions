# This Dockerfile builds the container from which presubmit tests are run
# Cloud Build orchestrates this process.

FROM gcr.io/cloud-builders/gcloud

RUN useradd -m -d /home/ia-tests -s /bin/bash ia-tests

COPY --chown=ia-tests:ia-tests . /init-actions

# Install Bazel:
# https://docs.bazel.build/versions/master/install-ubuntu.html
ENV bazel_kr_path=/usr/share/keyrings/bazel-keyring.gpg
RUN apt-get install -y -qq curl >/dev/null 2>&1 && \
    apt-get clean
RUN /usr/bin/curl https://bazel.build/bazel-release.pub.gpg | \
    gpg --dearmor -o "${bazel_kr_path}"
RUN echo "deb [arch=amd64 signed-by=${bazel_kr_path}] http://storage.googleapis.com/bazel-apt stable jdk1.8" | \
    dd of=/etc/apt/sources.list.d/bazel.list status=none && \
    apt-get update -qq
RUN apt-get autoremove -y -qq && \
    apt-get install -y -qq openjdk-8-jdk python3-setuptools >/dev/null 2>&1 && \
    apt-get clean

RUN curl -fsSL https://github.com/bazelbuild/bazel/releases/download/7.4.1/bazel-7.4.1-installer-linux-x86_64.sh -O /tmp/bazel-installer.sh && \
    chmod +x /tmp/bazel-installer.sh && \
    /tmp/bazel-installer.sh --user && \
    rm -f /tmp/bazel-installer.sh

ENV PATH="$PATH:$HOME/bin"

RUN echo "Bazel version:" && \
    bazel --version && \
    which bazel

USER ia-tests
