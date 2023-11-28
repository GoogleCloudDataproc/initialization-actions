Ensure that the Dataproc cluster also uses the property `dataproc:dataproc.localssd.mount.enable=false`  
```
gcloud dataproc clusters create demo-cluster \
--enable-component-gateway \
--region us-central1 --master-machine-type n2d-standard-2 --master-boot-disk-size 100 --num-master-local-ssds 2 \
--num-workers 2 --worker-machine-type n2d-standard-2 --worker-boot-disk-size 100 --num-worker-local-ssds 4 \
--image-version 2.1-debian11 \
--scopes 'https://www.googleapis.com/auth/cloud-platform' --project demo \
--properties dataproc:dataproc.localssd.mount.enable=false \
--initialization-actions 'gs://demo/dataproc-init-scripts/raid-disk.sh'
```
