#!/bin/bash
#    Copyright 2015 Google, Inc.
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
#    limitations under the License.set -x -e

# Only run on the master node
ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
if [[ "${ROLE}" == 'Master' ]]; then
	
	# Install dependencies needed for iPython Notebook
	apt-get install build-essential python-dev libpng-dev libfreetype6-dev libxft-dev pkg-config python-matplotlib python-requests python-numpy -y
	curl https://bootstrap.pypa.io/get-pip.py | python
	
	# Install iPython Notebook and create a profile
	mkdir IPythonNB
	cd IPythonNB
	pip install "ipython[notebook]"
	ipython profile create default
	
	# Set up configuration for iPython Notebook
	echo "c = get_config()" >  /root/.ipython/profile_default/ipython_notebook_config.py
	echo "c.NotebookApp.ip = '*'" >>  /root/.ipython/profile_default/ipython_notebook_config.py
	echo "c.NotebookApp.open_browser = False"  >>  /root/.ipython/profile_default/ipython_notebook_config.py
	echo "c.NotebookApp.port = 8123" >>  /root/.ipython/profile_default/ipython_notebook_config.py
	
	# Setup script for iPython Notebook so it uses the cluster's Spark
	cat > /root/.ipython/profile_default/startup/00-pyspark-setup.py <<'_EOF'
import os
import sys

spark_home = '/usr/lib/spark/'
os.environ["SPARK_HOME"] = spark_home
sys.path.insert(0, os.path.join(spark_home, 'python'))
sys.path.insert(0, os.path.join(spark_home, 'python/lib/py4j-0.8.2.1-src.zip'))
execfile(os.path.join(spark_home, 'python/pyspark/shell.py'))
_EOF
	
	# Start iPython Notebook on port 8123
	nohup ipython notebook --no-browser --ip=* --port=8123 > /var/log/python_notebook.log &
fi
