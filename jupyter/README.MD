# Jupyter Notebook

This folder contains initialization action to quickly setup and launch [Jupyter Notebook](http://jupyter.org/), (the successor of IPython notebook) along with [notebook extensions](https://github.com/ipython-contrib/IPython-notebook-extensions), and [RISE](https://github.com/damianavila/RISE). Jupyter is configured to run on port `8123` on the master node in the Dataproc cluster. To connect to the Jupyter Notebook web interface, you can simply configure and launch using `launch-jupyter-interface.sh`

## setup-jupyter.sh


`setup-jupyter.sh` sets sane defaults for configuring and running Jupyter on the Dataproc master node by:

- targeting a default notebooks directory 
- ensuring a Jupyter startup configuration for PySpark exists, and then 
- launches the Jupyter kernel on port 8123.


