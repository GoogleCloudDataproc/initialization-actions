package(default_visibility = ["//visibility:public"])

exports_files(["dask.sh"])

py_test(
    name = "test_dask",
    size = "enormous",
    srcs = ["test_dask.py"],
    data = [
        "dask.sh",
        "verify_dask_yarn.py",
        "verify_dask_standalone.py",
    ],
    local = True,
    shard_count = 4,
    deps = [
        "//integration_tests:dataproc_test_case",
        "@io_abseil_py//absl/testing:parameterized",
    ],
)
