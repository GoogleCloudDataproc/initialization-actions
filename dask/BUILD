package(default_visibility = ["//visibility:public"])

py_test(
    name = "test_dask",
    size = "enormous",
    srcs = ["test_dask.py"],
    data = [
        "dask.sh",
        "verify_dask.py",
    ],
    local = True,
    shard_count = 3,
    deps = [
        "//integration_tests:dataproc_test_case",
        "@io_abseil_py//absl/testing:parameterized",
    ],
)
