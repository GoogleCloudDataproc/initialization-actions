#!/bin/bash
ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
if [[ "${ROLE}" != "Master" ]]; then
  // Install mdadm
  sudo apt-get install mdadm

  // Create RAID 0 array
  sudo mdadm --create --verbose /dev/md0 --level=0 --raid-devices=4 /dev/sdb /dev/sdc /dev/sdd /dev/sde

  // Format the RAID 0 array
  sudo mkfs.ext4 /dev/md0

  // Create a mount point for the RAID 0 array
  sudo mkdir –p /mnt/raiddisk1

  // Mount the RAID 0 array
  sudo mount /dev/md0 /mnt/raiddisk1

  // Add the RAID 0 array to the /etc/fstab file
  echo "/dev/md0  /mnt/raiddisk1  ext4  defaults  0 0" | sudo tee -a /etc/fstab

  // Change the ownership of the RAID 0 array to the hdfs user
  sudo chown -R hdfs:hadoop /mnt/raiddisk1

  // Update the hdfs-site.xml file to use the RAID 0 array as the data directory
  sudo sed -i "/<name>dfs.datanode.data.dir<\/name>/{n;s|<value>.*</value>|<value>/mnt/raiddisk1</value>|;}" /etc/hadoop/conf/hdfs-site.xml

  // Update the spark-env.sh file to use the RAID 0 array as the local directory for Spark
  sed -i 's/export SPARK_LOCAL_DIRS=\/hadoop\/spark\/tmp/export SPARK_LOCAL_DIRS=\/mnt\/raiddisk1/' /etc/spark/conf/spark-env.sh

  // Restart the hadoop-hdfs-datanode service
  sudo systemctl restart hadoop-hdfs-datanode.service
fi
