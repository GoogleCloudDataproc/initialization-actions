package(default_visibility = ["//visibility:public"])

py_test(
    name = "test_dask",
    size = "enormous",
    srcs = ["test_dask.py"],
    data = [
        "dask.sh",
        "//gpu:install_gpu_driver.sh",
    ],
    local = True,
    shard_count = 3,
    deps = [
        ":verify_dask",
        "//integration_tests:dataproc_test_case",
        "@io_abseil_py//absl/testing:parameterized",
    ],
)

py_library(
    name = "verify_dask",
    testonly = True,
    srcs = ["verify_dask.py", "verify_dask_rapids.py"],
)
