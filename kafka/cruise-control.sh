#!/bin/bash
#    Copyright 2019 Google, Inc.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

# This script installs Cruise Control (https://github.com/linkedin/cruise-control)
# on a Dataproc Kafka cluster.
#
# Every node running Kafka broker will be updated to use the Cruise Control
# metric reporter, and Cruise Control server will be running on the first
# master node (port 9090 by default). By default, self healing is enabled for
# broker failure, goal violation and metric anomaly.

set -euxo pipefail

readonly CRUISE_CONTROL_HOME="/opt/cruise-control"
readonly CRUISE_CONTROL_CONFIG_FILE="${CRUISE_CONTROL_HOME}/config/cruisecontrol.properties"
readonly KAFKA_HOME=/usr/lib/kafka
readonly KAFKA_CONFIG_FILE='/etc/kafka/conf/server.properties'
readonly CRUISE_CONTROL_BUILD_SRC_FILE="${CRUISE_CONTROL_HOME}/buildSrc"

readonly ROLE="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"
readonly CRUISE_CONTROL_VERSION="$(/usr/share/google/get_metadata_value attributes/cruise-control-version || echo 2.0.37)"
readonly CRUISE_CONTROL_HTTP_PORT="$(/usr/share/google/get_metadata_value attributes/cruise-control-http-port || echo 9090)"
readonly SELF_HEALING_BROKER_FAILURE_ENABLED="$(/usr/share/google/get_metadata_value attributes/self-healing-broker-failure-enabled || echo true)"
readonly SELF_HEALING_GOAL_VIOLATION_ENABLED="$(/usr/share/google/get_metadata_value attributes/self-healing-goal-violation-enabled || echo true)"
readonly SELF_HEALING_METRIC_ANOMALY_ENABLED="$(/usr/share/google/get_metadata_value attributes/self-healing-metric-anomaly-enabled || echo false)"
readonly BROKER_FAILURE_ALERT_THRESHOLD_MS="$(/usr/share/google/get_metadata_value attributes/broker-failure-alert-threshold-ms || echo 120000)"
readonly BROKER_FAILURE_SELF_HEALING_THRESHOLD_MS="$(/usr/share/google/get_metadata_value attributes/broker-failure-alert-threshold-ms || echo 300000)"

function download_cruise_control() {
  mkdir -p /opt
  pushd /opt
  git clone --branch ${CRUISE_CONTROL_VERSION} --depth 1 https://github.com/linkedin/cruise-control.git
  popd
}

function update_jfrog_repository() {
  pushd ${CRUISE_CONTROL_BUILD_SRC_FILE}
  updated_jfrog_version="4.23.4"
  sed "s/\(org.jfrog.buildinfo:build-info-extractor-gradle:\)[^']*/\1$updated_jfrog_version/" build.gradle | sudo tee temp > /dev/null && sudo mv temp build.gradle
  popd
  pushd ${CRUISE_CONTROL_HOME}
  sudo tee build.gradle << 'EOF'
/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

plugins {
  id "idea"
}

group = 'com.linkedin.cruisecontrol'

project.ext {
  pomConfig = {
    url "https://github.com/linkedin/cruise-control"
    licenses {
      license {
        name "BSD 2-CLAUSE LICENSE"
        url "https://opensource.org/licenses/BSD-2-Clause"
      }
    }
    developers {
      developer {
        name "Adem Efe Gencer"
        email "efegencer@gmail.com"
      }
      developer {
        name "Jiangjie (Becket) Qin"
        email "becket.qin@gmail.com"
      }
      developer {
        name "Sir Joel Koshy"
        email "jjkoshy@yahoo.com"
      }
    }
    scm {
      url "https://github.com/linkedin/cruise-control"
    }
  }
  buildVersionFileName = "cruise-control-version.properties"
  commitId = project.hasProperty('commitId') ? commitId : null
}

allprojects {

  repositories {
    mavenCentral()
    jcenter()
  }
}

subprojects {
  group = rootProject.group
  version = rootProject.version

  apply plugin: 'java'
  apply plugin: 'checkstyle'
  apply plugin: 'findbugs'

  test.dependsOn('checkstyleMain', 'checkstyleTest')

  task sourcesJar(type: Jar, dependsOn: classes) {
    classifier = 'sources'
    from sourceSets.main.allSource
  }

  task javadocJar(type: Jar, dependsOn: javadoc) {
    classifier = 'javadoc'
    from javadoc.destinationDir
  }

  //code quality and inspections
  checkstyle {
    toolVersion = '7.5.1'
    ignoreFailures = false
    configFile = rootProject.file('checkstyle/checkstyle.xml')
  }

  findbugs {
    toolVersion = "3.0.1"
    excludeFilter = file("$rootDir/gradle/findbugs-exclude.xml")
    ignoreFailures = false
  }

  test.dependsOn('checkstyleMain', 'checkstyleTest', 'findbugsMain', 'findbugsTest')

  tasks.withType(FindBugs) {
    reports {
      xml.enabled (project.hasProperty('xmlFindBugsReport'))
      html.enabled (!project.hasProperty('xmlFindBugsReport'))
    }
  }

  jar {
    from "$rootDir/LICENSE"
    from "$rootDir/NOTICE"
  }

  test {
    useJUnit {}
    testLogging {
      events "passed", "failed", "skipped"
      exceptionFormat = 'full'
    }
    if (!project.hasProperty("maxParallelForks")) {
      maxParallelForks = Runtime.runtime.availableProcessors()
    }
  }
}

project(':cruise-control-core') {
  apply plugin: 'maven-publish'

  configurations {
    testOutput
  }

  dependencies {
    compile "org.slf4j:slf4j-api:1.7.25"
    compile 'junit:junit:4.12'
    compile 'org.apache.commons:commons-math3:3.6.1'

    testOutput sourceSets.test.output
  }

  publishing {
    publications {
      java(MavenPublication) {
        from components.java
        artifact sourcesJar
        artifact javadocJar
        pom.withXml {
          def root = asNode()
          root.appendNode('name', 'cruise-control-core')
          root.appendNode('description', 'cruise control core related')
          root.children().last() + rootProject.ext.pomConfig
        }
      }
    }
  }

  sourceSets {
    main {
      java {
        srcDirs = ['src/main/java']
      }
    }
    test {
      java {
        srcDirs = ['src/test/java']
      }
    }
  }
}

project(':cruise-control') {
  apply plugin: 'scala'
  apply plugin: 'maven-publish'

  //needed because our java classes depend on scala classes, so must be compiled by scala
  sourceSets {
    main {
      java {
        srcDirs = []
      }

      scala {
        srcDirs = ['src/main/java', 'src/main/scala']
      }
    }

    test {
      java {
        srcDirs = []
      }
      scala {
        srcDirs = ['src/test/java', 'src/test/scala']
      }
    }
  }

  dependencies {
    compile project(':cruise-control-metrics-reporter')
    compile project(':cruise-control-core')
    compile "org.slf4j:slf4j-api:1.7.25"
    compile "org.apache.zookeeper:zookeeper:3.4.6"
    compile "org.apache.kafka:kafka_2.11:2.0.0"
    compile 'org.apache.kafka:kafka-clients:2.0.0'
    compile "org.scala-lang:scala-library:2.11.11"
    compile 'junit:junit:4.12'
    compile 'org.apache.commons:commons-math3:3.6.1'
    compile 'com.google.code.gson:gson:2.7'
    compile 'org.eclipse.jetty:jetty-server:9.4.6.v20170531'
    compile 'org.eclipse.jetty:jetty-servlet:9.4.6.v20170531'
    compile 'io.dropwizard.metrics:metrics-core:3.2.2'

    testCompile project(path: ':cruise-control-metrics-reporter', configuration: 'testOutput')
    testCompile project(path: ':cruise-control-core', configuration: 'testOutput')
    testCompile "org.scala-lang:scala-library:2.11.11"
    testCompile 'org.easymock:easymock:3.4'
    testCompile 'org.apache.kafka:kafka_2.11:2.0.0:test'
    testCompile 'org.apache.kafka:kafka-clients:2.0.0:test'
    testCompile 'commons-io:commons-io:2.5'
  }

  publishing {
    publications {
      java(MavenPublication) {
        from components.java
        artifact sourcesJar
        artifact javadocJar
        pom.withXml {
          def root = asNode()
          root.appendNode('name', 'cruise-control')
          root.appendNode('description', 'cruise control related')
          root.children().last() + rootProject.ext.pomConfig
        }
      }
    }
  }

  tasks.create(name: "copyDependantLibs", type: Copy) {
    from (configurations.testRuntime) {
      include('slf4j-log4j12*')
    }
    from (configurations.runtime) {

    }
    into "$buildDir/dependant-libs"
    duplicatesStrategy 'exclude'
  }

  tasks.create(name: "buildFatJar", type: Jar) {
    baseName = project.name + '-all'
    from { configurations.compile.collect { it.isDirectory() ? it : zipTree(it) } }
    with jar
  }

  compileScala.doLast {
    tasks.copyDependantLibs.execute()
  }

  task determineCommitId {
    def takeFromHash = 40
    if (commitId) {
      commitId = commitId.take(takeFromHash)
    } else if (file("$rootDir/.git/HEAD").exists()) {
      def headRef = file("$rootDir/.git/HEAD").text
      if (headRef.contains('ref: ')) {
        headRef = headRef.replaceAll('ref: ', '').trim()
        if (file("$rootDir/.git/$headRef").exists()) {
          commitId = file("$rootDir/.git/$headRef").text.trim().take(takeFromHash)
        }
      } else {
        commitId = headRef.trim().take(takeFromHash)
      }
    } else {
      commitId = "unknown"
    }
  }

  // Referenced similar method for getting software version in Kafka code.
  task createVersionFile(dependsOn: determineCommitId) {
    ext.receiptFile = file("$buildDir/cruise-control/$buildVersionFileName")
    outputs.file receiptFile
    outputs.upToDateWhen { false }
    doLast {
      def data = [
          commitId: commitId,
          version: version,
      ]

      receiptFile.parentFile.mkdirs()
      def content = data.entrySet().collect { "$it.key=$it.value" }.sort().join("\n")
      receiptFile.setText(content, "ISO-8859-1")
    }
  }

  jar {
    dependsOn createVersionFile
    from("$buildDir") {
      include "cruise-control/$buildVersionFileName"
    }
  }
}

project(':cruise-control-metrics-reporter') {
  apply plugin: 'maven-publish'

  configurations {
    testOutput
  }

  dependencies {
    compile "org.slf4j:slf4j-api:1.7.25"
    compile "org.apache.kafka:kafka_2.11:2.0.0"
    compile 'org.apache.kafka:kafka-clients:2.0.0'
    compile 'junit:junit:4.12'
    compile 'org.apache.commons:commons-math3:3.6.1'

    testCompile 'org.bouncycastle:bcpkix-jdk15on:1.54'
    testCompile 'org.apache.kafka:kafka-clients:2.0.0:test'
    testCompile 'org.apache.kafka:kafka-clients:2.0.0'
    testCompile 'commons-io:commons-io:2.5'
    testOutput sourceSets.test.output
  }

  publishing {
    publications {
      java(MavenPublication) {
        from components.java
        artifact sourcesJar
        artifact javadocJar
        pom.withXml {
          def root = asNode()
          root.appendNode('name', 'cruise-control-metrics-reporter')
          root.appendNode('description', 'cruise control metrics reporter related')
          root.children().last() + rootProject.ext.pomConfig
        }
      }
    }
  }
}

//wrapper generation task
task wrapper(type: Wrapper) {
  gradleVersion = '4.8'
  distributionType = Wrapper.DistributionType.ALL
}
EOF
  popd
}

function build_cruise_control() {
  pushd ${CRUISE_CONTROL_HOME}
  ./gradlew jar copyDependantLibs
  popd
}

function update_kafka_metrics_reporter() {
  if [[ ! -d "${CRUISE_CONTROL_HOME}" ]]; then
    echo "Kafka is not installed on this node ${HOSTNAME}, skip configuring Cruise Control."
    return 0
  fi

  cp ${CRUISE_CONTROL_HOME}/cruise-control-metrics-reporter/build/libs/cruise-control-metrics-reporter-2.0.38-SNAPSHOT.jar \
    ${KAFKA_HOME}/libs
  cat >>${KAFKA_CONFIG_FILE} <<EOF

# Properties added by Cruise Control init action.
metric.reporters=com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporter
EOF

  systemctl restart kafka-server
}

function configure_cruise_control_server() {
  echo "Configuring cruise control server."
  cat >>"${CRUISE_CONTROL_CONFIG_FILE}" <<EOF

# Properties from the Cruise Control init action.
webserver.http.port=${CRUISE_CONTROL_HTTP_PORT}
self.healing.broker.failure.enabled=${SELF_HEALING_BROKER_FAILURE_ENABLED}
self.healing.goal.violation.enabled=${SELF_HEALING_GOAL_VIOLATION_ENABLED}
self.healing.metric.anomaly.enabled=${SELF_HEALING_METRIC_ANOMALY_ENABLED}
broker.failure.alert.threshold.ms=${BROKER_FAILURE_ALERT_THRESHOLD_MS}
broker.failure.self.healing.threshold.ms=${BROKER_FAILURE_SELF_HEALING_THRESHOLD_MS}
anomaly.detection.goals=com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaCapacityGoal,com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskCapacityGoal,com.linkedin.kafka.cruisecontrol.analyzer.goals.CpuCapacityGoal,com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaDistributionGoal
metric.anomaly.finder.class=com.linkedin.kafka.cruisecontrol.detector.NoopMetricAnomalyFinder

EOF
}

function start_cruise_control_server() {
  # Wait for the metrics topic to be created.
  for ((i = 1; i <= 20; i++)); do
    local metrics_topic=$(/usr/lib/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092 | grep __CruiseControlMetrics)
    if [[ -n "${metrics_topic}" ]]; then
      break
    else
      echo "Metrics topic __CruiseControlMetrics is not created yet, retry $i ..."
      sleep 5
    fi
  done

  if [[ -z "${metrics_topic}" ]]; then
    err "Metrics topic __CruiseControlMetrics was not found in the cluster."
  fi

  echo "Start Cruise Control server on ${HOSTNAME}."
  pushd ${CRUISE_CONTROL_HOME}
  ./kafka-cruise-control-start.sh config/cruisecontrol.properties &
  popd
}

function main() {
  download_cruise_control
  update_jfrog_repository
  build_cruise_control
  update_kafka_metrics_reporter
  # Run CC on the first master node.
  if [[ "${HOSTNAME}" == *-m || "${HOSTNAME}" == *-m-0 ]]; then
    configure_cruise_control_server
    start_cruise_control_server
  fi
}

main
