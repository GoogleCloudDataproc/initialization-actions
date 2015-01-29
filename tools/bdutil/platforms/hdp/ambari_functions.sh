## Tools for interacting with Ambari SERVER

AMBARI_TIMEOUT=${AMBARI_TIMEOUT:-3600}
POLLING_INTERVAL=${POLLING_INTERVAL:-10}


function ambari_wait() {
  local condition="$1"
  local goal="$2"
  local failed="FAILED"
  local limit=$(( ${AMBARI_TIMEOUT} / ${POLLING_INTERVAL} + 1 ))

  for (( i=0; i<${limit}; i++ )); do
    local status=$(bash -c "${condition}")
    if [ "${status}" = "${goal}" ]; then
      break
    elif [ "${status}" = "${failed}" ]; then
      echo "Ambari operiation failed with status: ${status}" >&2
      return 1
    fi
    echo "ambari_wait status: ${status}" >&2
    sleep ${POLLING_INTERVAL}
  done

  if [ ${i} -eq ${limit} ]; then
    echo "ambari_wait did not finish within" \
        "'${AMBARI_TIMEOUT}' seconds. Exiting." >&2
    return 1
  fi
}

# Only useful during a fresh install where we expect no failures
# Will not work if any requested FAILED/TIMEDOUT/ABORTED
function ambari_wait_requests_completed() {
      # Poll for completion
      ambari_wait "${AMBARI_CURL} ${AMBARI_API}/clusters/${PREFIX}/requests \
            | grep -Eo 'http://.*/requests/[^\"]+' \
            | tail -1 \
            | xargs ${AMBARI_CURL} \
            | grep request_status \
            | uniq \
            | tr -cd '[:upper:]'" \
            'COMPLETED'
}

function ambari_service_stop() {
    if [ -x ${SERVICE+x} ]; then
        echo "Taking no action as no SERVICE was defined. You may specific ALL to stop all Services."
    else
        AMBARI_REQUEST='{"RequestInfo": {"context" :"Stop '${SERVICE}' via REST"}, "Body": {"ServiceInfo": {"state": "INSTALLED"}}}'
        if [ "${SERVICE}" = "ALL" ]; then
            ${AMBARI_CURL} -i -X PUT -d "${AMBARI_REQUEST}" ${AMBARI_API}/clusters/${PREFIX}/services/
        else
            ${AMBARI_CURL} -i -X PUT -d "${AMBARI_REQUEST}" ${AMBARI_API}/clusters/${PREFIX}/services/${SERVICE}
        fi
    fi
}

function ambari_service_start() {
    if [ -x ${SERVICE+x} ]; then
        echo "Taking no action as no SERVICE was defined"
    else
        AMBARI_REQUEST='{"RequestInfo": {"context" :"Start '${SERVICE}' via REST"}, "Body": {"ServiceInfo": {"state": "STARTED"}}}'
        if [ "${SERVICE}" = "ALL" ]; then
            ${AMBARI_CURL} -i -X PUT -d "${AMBARI_REQUEST}" ${AMBARI_API}/clusters/${PREFIX}/services/
        else
            ${AMBARI_CURL} -i -X PUT -d "${AMBARI_REQUEST}" ${AMBARI_API}/clusters/${PREFIX}/services/${SERVICE}
        fi
    fi
}

# set SERVICE=ALL to restart all services
function ambari_service_restart() {
    ambari_service_stop
    ambari_wait_requests_completed
    ambari_service_start
    ambari_wait_requests_completed
}

function ambari_restart_all_services() {
    SERVICES=($(${AMBARI_CURL} ${AMBARI_API}/clusters/${PREFIX}/services \
        | grep -Eo 'http://.*/services/[^\"]+'))

    for STATE in 'INSTALLED' 'STARTED'; do
      ${AMBARI_CURL} -X PUT -d "{\"ServiceInfo\":{\"state\":\"${STATE}\"}}" "${SERVICES[@]}"
      ambari_wait_requests_completed
    done
}
