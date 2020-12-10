py_test(
    name = "test_mlvm",
    size = "enormous",
    srcs = ["test_mlvm.py"],
    data = [
        "mlvm.sh", 
        "scripts/python_packages.py",
        "scripts/r_packages.R",
        "scripts/spark_bq.py",
        "scripts/verify_dask_standalone.py",
        "scripts/verify_dask_yarn.py",
        "scripts/verify_rapids_dask.py",
        "scripts/verify_rapids_spark.py",
        "//dask:dask.sh",
        "//gpu:install_gpu_driver.sh",
        "//rapids:rapids.sh",
    ],
    local = True,
    shard_count = 7,
    deps = [
        "//integration_tests:dataproc_test_case",
        "@io_abseil_py//absl/testing:parameterized",
    ],
)
