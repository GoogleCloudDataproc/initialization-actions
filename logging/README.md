# Node Manager Logs

This init action changes the default path of the node manager logs (/var/log/hadoop-yarn/userlogs) to the Local SSD mounts(/mnt/<mnt>/<path>). 
The script provides a couple of configurations that can be controlled as needed.
It also configures FluentD to look for the node manager logs in the Local SSD mounts


## Usage

**:warning: NOTICE:** See [best practices](/README.md#how-initialization-actions-are-used) of using initialization actions in production.

You can create a cluster with Local SSD mounts using a command like the following. Without the initialisation script, all node manager logs would be written to the boot disk.

```bash
REGION=<region>
CLUSTER_NAME=<cluster_name>
gcloud dataproc clusters create ${CLUSTER_NAME?} \
  --region ${REGION?} \
  --image-version=2.0 \
  --enable-component-gateway \
  --num-worker-local-ssds 2 \
  --initialization-actions gs://goog-dataproc-initialization-actions-${REGION?}/logging/change_to_local_ssd.sh
```


## Notes
- Update the value of MAX_MNT_DISK_FOR_LOGS to specify the total number of local SSDs we want to use for logging. I've currently set the default to 3 but you can change according to the workload
- Update LOGPATH to the required path under /mnt// to specify the logging directory. I've currently set it to hadoop/yarn/userlogs. So logs will go to /mnt//hadoop/yarn/userlogs
- If there are no local SSDs, the script will not add any property to yarn-site and the default boot disk path will be used
- If there are multiple mounts, it will create a comma separated list of paths for the property based on the MAX_MNT_DISK_FOR_LOGS setting.
- If MAX_MNT_DISK_FOR_LOGS is greater than the actual disks, then the actual disk count will be used. If not, the configuration will be honoured.