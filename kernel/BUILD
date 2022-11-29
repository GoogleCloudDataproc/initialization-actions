package(default_visibility = ["//visibility:public"])

exports_files(["upgrade-kernel.sh", "revert-kernel.sh"])

py_test(
    name = "test_kernel",
    size = "medium",
    timeout = "eternal",
    srcs = ["test_kernel.py"],
    data = ["upgrade-kernel.sh", "revert-kernel.sh"],
    local = True,
    shard_count = 2,
    deps = [
        "//integration_tests:dataproc_test_case",
        "@io_abseil_py//absl/testing:parameterized",
    ],
)
