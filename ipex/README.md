
## In Intel's own words:

Intel® Extension for PyTorch* extends PyTorch* with the latest
performance optimizations for Intel hardware. Optimizations take
advantage of Intel® Advanced Vector Extensions 512 (Intel® AVX-512)
Vector Neural Network Instructions (VNNI) and Intel® Advanced Matrix
Extensions (Intel® AMX) on Intel CPUs as well as Intel XeMatrix
Extensions (XMX) AI engines on Intel discrete GPUs. Moreover, Intel®
Extension for PyTorch* provides easy GPU acceleration for Intel
discrete GPUs through the PyTorch* xpu device.

The extension can be loaded as a Python module for Python programs or
linked as a C++ library for C++ programs. In Python scripts, users can
enable it dynamically by importing intel_extension_for_pytorch.

## This action:

The ipex initialization action installs the appropriate python
libraries and installs the spark-tfrecord jar for the current scala
runtime into the cluster's classpath.

### Metadata arguments
* metadata name: spark-tfrecord-version
* default value: 0.4.0
  Used to specify the version of the spark-tfrecord jar.  See url
  https://central.sonatype.com/artifact/com.linkedin.sparktfrecord/spark-tfrecord_2.12/versions
  https://central.sonatype.com/artifact/com.linkedin.sparktfrecord/spark-tfrecord_2.13/versions

* metadata name: scala-version
* defaut value: 2.12
  Used to specify the version of scala.  See url
  https://github.com/scala/scala/releases
