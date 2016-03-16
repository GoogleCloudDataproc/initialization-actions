# Jupyter Notebook

This folder contains initialization action to quickly setup and launch [Jupyter Notebook](http://jupyter.org/), (the successor of IPython notebook) along with [notebook extensions](https://github.com/ipython-contrib/IPython-notebook-extensions), and [RISE](https://github.com/damianavila/RISE). Jupyter is configured, by default, to run on port `8123` on the master node in the Dataproc cluster. The port can be overridden by setting the *metadata* property `JUPYTER_PORT` to a valid port number during cluster creation. For instance:

```
gcloud dataproc clusters create my-awesome-cluster --metadata JUPYTER_PORT=8124 ...
```

To connect to the Jupyter Notebook web interface, you can simply configure and launch using `launch-jupyter-interface.sh` from your local machine.

## setup-jupyter-kernel.sh

`setup-jupyter-kernel.sh` sets sane defaults for configuring and running Jupyter on the Dataproc master node

- configures `jupyter` to use the *PySpark* kernel found at `./kernels/pyspark/kernel.json`.
- targets a default notebooks directory `/root/notebooks`.

**NOTE**: to be run as an init action.


## launch-jupyter-kernel.sh

`launch-jupyter-kernel.sh` simply launches the Jupyter server on port `8123`, by default, or the port specified by the *metadata* value `JUPYTER_PORT` on the master node.

**NOTE**: to be run as an init action.


## bootstrap-jupyter-ext.sh

`bootstrap-jupyter-ext.sh` sets up and installs notebook extensions and RISE. 

**NOTE**: to be run as an init action.


## launch-jupyter-interface.sh

`launch-jupyter-interface.sh` launches a web interface to connect to Jupyter notebook process running on master node. 

- sets a path for the local OS to the Chrome executable
- setup an ssh tunnel and socks proxy to the master node
- launch a Chrome instance that uses this ssh tunnel and references the Jupyter port.

**NOTE**: to be run from a local machine
 
