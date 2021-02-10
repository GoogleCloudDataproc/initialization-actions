package(default_visibility = ["//visibility:public"])

py_test(
    name = "test_tony",
    size = "enormous",
    srcs = ["test_tony.py"],
    data = [
        "tony.sh",
        "//gpu:install_gpu_driver.sh"
    ],
    local = True,
    shard_count = 4,
    deps = [
        "//integration_tests:dataproc_test_case",
        "@io_abseil_py//absl/testing:parameterized",
    ],
)
