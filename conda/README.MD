# Miniconda (a Python distro and package manager, from Continuum Analytics)

This folder contains initialization actions for getting up and going using [Miniconda](http://conda.pydata.org/miniconda.html). Miniconda is a shell script installer that contains both:
 
- [Anaconda](https://www.continuum.io/why-anaconda), a barebones version of a completely open (and legit) Python distro from Continuum Analytics, alongside,
- [conda](http://conda.pydata.org/docs/), a completely open (and amazing) package manager.
 

## bootstrap-conda.sh

`bootstrap-conda.sh` contains logic for quickly configuring and installing Miniconda, with point-and-shoot defaults, including:

- the Python 3 variant
- the most recent Miniconda version (`3.18.3`)
- downloads and installs to the `$HOME` directory
- performs an md5sum hash check (failing quickly is always better)
- updates current `$PATH` and subsequent definitions in `.bashrc`
- updates `conda` and install `pip` in root environment (allowing pip installations)
- installs some powerful extensions:
    - [`conda-env`](https://github.com/conda/conda-env)
    - [`conda-build`](https://github.com/conda/conda-build)
    - [`anaconda-client`](https://github.com/Anaconda-Server/anaconda-client)
    
 
All of this can be changed with little effort.

## install-conda-env.sh

`install-conda-env.sh` contains logic for creating a conda environment and installing conda (and pip) packages. Sane defaults include:

- installs some common libraries (pandas, scikit-learn, bokeh, plotly, Jupyter) 
- if no conda environment name is specified, uses `root`.
- detects if conda environment has already been created.
- updates `.bashrc` to activate the created environment at login
