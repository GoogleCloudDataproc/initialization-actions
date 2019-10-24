package(default_visibility = ["//visibility:public"])

py_test(
    name = "test_flink",
    size = "enormous",
    srcs = ["test_flink.py"],
    data = [
        "flink.sh",
        "validate.sh",
    ],
    local = True,
    shard_count = 5,
    deps = [
        "//integration_tests:dataproc_test_case",
        "@io_abseil_py//absl/testing:parameterized",
    ],
)
