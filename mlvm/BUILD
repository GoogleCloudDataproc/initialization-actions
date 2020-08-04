py_test(
    name = "test_mlvm",
    size = "enormous",
    srcs = ["test_mlvm.py"],
    data = [
        "mlvm.sh", 
        "scripts/python_packages.py",
        "scripts/r_packages.R",
        "scripts/spark_bq.py",        
        "scripts/verify_rapids_dask.py",
        "scripts/verify_rapids_spark.py",
        "//gpu:install_gpu_driver.sh",
        "//rapids:rapids.sh",
    ],
    local = True,
    shard_count = 5,
    deps = [
        "//integration_tests:dataproc_test_case",
        "@io_abseil_py//absl/testing:parameterized",
    ],
)
