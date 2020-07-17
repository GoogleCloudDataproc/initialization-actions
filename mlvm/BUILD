py_test(
    name = "test_mlvm",
    size = "enormous",
    srcs = ["test_mlvm.py"],
    data = [
        "mlvm.sh", 
        "scripts/python_packages.py",
        "scripts/r_packages.R",
        "scripts/sample-h2o-script.py",
        "scripts/verify_rapids_dask.py",
        "scripts/verify_rapids_spark.py"],
    local = True,
    shard_count = 26,
    deps = [
        "//integration_tests:dataproc_test_case",
        "@io_abseil_py//absl/testing:parameterized",
    ],
)
