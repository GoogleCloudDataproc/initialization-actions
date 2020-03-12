package(default_visibility = ["//visibility:public"])

py_test(
    name = "test_connectors",
    size = "enormous",
    srcs = ["test_connectors.py"],
    data = ["connectors.sh"],
    local = True,
    shard_count = 10,
    deps = [
        "//integration_tests:dataproc_test_case",
        "@io_abseil_py//absl/testing:parameterized",
    ],
)
