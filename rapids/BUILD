package(default_visibility = ["//visibility:public"])

exports_files(["rapids.sh"])

py_test(
    name = "test_rapids",
    size = "enormous",
    srcs = ["test_rapids.py"],
    data = [
        "rapids.sh",
        "//gpu:install_gpu_driver.sh",
    ],
    local = True,
    shard_count = 6,
    deps = [
        ":verify_rapids",
        "//integration_tests:dataproc_test_case",
        "@io_abseil_py//absl/testing:parameterized",
    ],
)

py_library(
    name = "verify_rapids",
    testonly = True,
    srcs = ["verify_rapids_dask.py"],
)
