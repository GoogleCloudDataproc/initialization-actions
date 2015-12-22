# Jupyter Notebook

This folder contains initialization action to quickly setup and launch [Jupyter Notebook](http://jupyter.org/), (the successor of IPython notebook) along with [notebook extensions](https://github.com/ipython-contrib/IPython-notebook-extensions), and [RISE](https://github.com/damianavila/RISE). Jupyter is configured to run on port `8123` on the master node in the Dataproc cluster. To connect to the Jupyter Notebook web interface, you can simply configure and launch using `launch-jupyter-interface.sh`

## setup-jupyter.sh

`setup-jupyter.sh` sets sane defaults for configuring and running Jupyter on the Dataproc master node by:

- targets a default notebooks directory 
- ensures a Jupyter startup configuration for PySpark exists, and then 
- launches the Jupyter kernel on port 8123.

## launch-jupyter-interface.sh

`launch-jupyter-interface.sh` launches a web interface to Jupyter proces running on master node. 

- sets a path for the local OS to the Chrome executable
- setup an ssh tunnel and socks proxy to the master node
- launch a Chrome instance that uses this ssh tunnel and references the Jupyter port.

