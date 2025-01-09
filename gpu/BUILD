package(default_visibility = ["//visibility:public"])

exports_files(["install_gpu_driver.sh", "mig.sh"])

py_test(
    name = "test_gpu",
    size = "enormous",
    srcs = ["test_gpu.py"],
    data = ["install_gpu_driver.sh", "mig.sh"],
    local = True,
    shard_count = 15,
    deps = [
        "//integration_tests:dataproc_test_case",
        "@io_abseil_py//absl/testing:parameterized",
    ],
)
