#!/bin/bash

set -euxo pipefail

ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
NAME=$(hostname)
if [[ "${ROLE}" == 'Master' ]]; then
  CUSTOM_LOGGING_PROPS_FILE=/etc/hadoop/conf/disable-noisy-timeline-logger.logging.properties
  cat << EOF > "${CUSTOM_LOGGING_PROPS_FILE}"
com.sun.jersey.server.wadl.generators.WadlGeneratorJAXBGrammarGenerator.level=OFF
EOF

  cat << EOF >> /etc/hadoop/conf/yarn-env.sh
export YARN_TIMELINESERVER_OPTS="\${YARN_TIMELINESERVER_OPTS} -Djava.util.logging.config.file=${CUSTOM_LOGGING_PROPS_FILE}"
EOF

  systemctl restart hadoop-yarn-timelineserver
  systemctl status --no-pager hadoop-yarn-timelineserver
  rm /var/log/hadoop-yarn/yarn-yarn-timelineserver-${NAME}.out.*
fi
