### Prep
```
CONFIGBUCKET=hdp-play-00
PROJECT=hdp-play-00

switches="-b ${CONFIGBUCKET} -p ${PROJECT}
    --master_attached_pd_size_gb 100
    --worker_attached_pds_size_gb 100
    -n 4
    -m n1-standard-2"

bdutil="./bdutil ${switches}"
```

### Test ambari_env.sh

```
environment=platforms/hdp/ambari_env.sh
${bdutil} -e ${environment} deploy
${bdutil} shell < ./hadoop-validate-setup.sh
${bdutil} shell < ./hadoop-validate-gcs.sh
${bdutil} shell < ./extensions/querytools/hive-validate-setup.sh
${bdutil} shell < ./extensions/querytools/pig-validate-setup.sh
#${bdutil} shell < ./extensions/spark/spark-validate-setup.sh
${bdutil} -e ${environment} delete
```


# Test ambari_manual_env.sh
```
environment=platforms/hdp/ambari_manual_env.sh
${bdutil} -e ${environment} deploy
# need to add an automated test here:
    ${bdutil} shell # do something here like check the appropriate number of hosts in /api/v1/hosts
${bdutil} -e ${environment} delete

```
