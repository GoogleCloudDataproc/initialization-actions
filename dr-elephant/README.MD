# Dr. Elephant

This script installs Dr. Elephant on dataproc clusters. To learn more about Dr. Elephant see original github page: https://github.com/linkedin/dr-elephant.


## Using this initialization action
You can use this initialization action to create a new Dataproc cluster with Dr. Elephant installed.

Once the cluster has been created, Dr. Elephant is configured to run on port `8080`.

## Testing
You can easily test Dr. Elephant is running by using ```test.sh ${cluster_prefix} ${bucket_name}``` script.

* ```cluster_prefix``` argument sets a prefix in created cluster name
* ```bucket_name``` argument sets a place where init action will be placed

This script is used for testing dr-elephant on 1.1,1.2,1.3 dataproc images with single,
standard and HA configurations. After clusters are created, script submit spark jobs on them.

Dr. Elephant UI can be accessed after connection with command:
```gcloud compute ssh ${cluster_prefix}-*config*-${suffix}-m* -- -L 8080:${cluster_prefix}-*config*-${suffix}-m*:8080```

Then just open a browser and type localhost:8080 address. Your jobs statistics should be there.
