#!/bin/bash
echo "BEGIN Stage 2 : Tune Spark default conf for H2O"

CLUSTER_NAME="$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)"
ZONE=$(gcloud compute instances list --filter="name=`hostname`" --format "value(zone)")
REGION=$(gcloud beta compute instances describe `hostname` --format='default(labels)' --zone=$ZONE | grep "goog-dataproc-location" | cut -d ':' -f 2)

cat << EOF > /usr/local/bin/await_cluster_and_run_command.sh
#!/bin/bash
# Helper to get current cluster state.
function get_cluster_state() {
        echo \$(gcloud dataproc clusters describe ${CLUSTER_NAME} --region ${REGION} | grep -A 1 "^status:" | grep "state:" | cut -d ':' -f 2)
}
# Helper which waits for RUNNING state before issuing the command.
function await_and_submit() {
        local cluster_state=\$(get_cluster_state)
        echo "Current cluster state: \${cluster_state}"
        while [[ "\${cluster_state}" != 'RUNNING' ]]; do
        echo "Sleeping to await cluster health..."
        sleep 5
        local cluster_state=\$(get_cluster_state)
        if [[ "\${cluster_state}" == 'ERROR' ]]; then
          echo "Giving up due to cluster state '\${cluster_state}'"
          exit 1
        fi
        done

        echo "Changing Spark Configurations"
        sudo sed -i 's/spark.dynamicAllocation.enabled=true/spark.dynamicAllocation.enabled=false/g' /usr/lib/spark/conf/spark-defaults.conf
        sudo sed -i 's/spark.executor.instances=10000/# spark.executor.instances=10000/g' /usr/lib/spark/conf/spark-defaults.conf
        sudo sed -i 's/spark.executor.cores.*/# removing unnecessary limits to executor cores/g' /usr/lib/spark/conf/spark-defaults.conf
        sudo sed -i 's/spark.executor.memory.*/# removing unnecessary limits to executor memory/g' /usr/lib/spark/conf/spark-defaults.conf
        sudo echo "spark.executor.instances=$(gcloud dataproc clusters describe ${CLUSTER_NAME} --region ${REGION} | grep "numInstances:" | tail -1 | sed "s/.*numInstances: //g")" >> /usr/lib/spark/conf/spark-defaults.conf
        sudo echo "spark.executor.cores=$(gcloud compute machine-types describe $(gcloud dataproc clusters describe ${CLUSTER_NAME} --region ${REGION} | grep "machineTypeUri" | tail -1 | sed 's/.*machineTypeUri: //g') | grep "guestCpus" | sed 's/guestCpus: //g')" >> /usr/lib/spark/conf/spark-defaults.conf
        sudo echo "spark.executor.memory=$(($(gcloud compute machine-types describe $(gcloud dataproc clusters describe ${CLUSTER_NAME} --region ${REGION} | grep "machineTypeUri" | tail -1 | sed 's/.*machineTypeUri: //g') | grep "memoryMb:" | sed 's/memoryMb: //g') * 65 / 100))m" >> /usr/lib/spark/conf/spark-defaults.conf
        echo "Successfully Changed spark-defaults.conf"

        cat /usr/lib/spark/conf/spark-defaults.conf
}

await_and_submit
EOF

chmod 750 /usr/local/bin/await_cluster_and_run_command.sh
nohup /usr/local/bin/await_cluster_and_run_command.sh &>> /var/log/master-post-init.log &
echo "END Stage 2 : Tune Spark default conf for H2O"
