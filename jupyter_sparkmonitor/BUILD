package(default_visibility = ["//visibility:public"])

py_test(
    name = "test_jupyter_sparkmonitor",
    size = "enormous",
    srcs = ["test_jupyter_sparkmonitor.py"],
    data = ["sparkmonitor.sh"],
    local = True,
    shard_count = 2,
    deps = [
        "//integration_tests:dataproc_test_case",
        "@io_abseil_py//absl/testing:parameterized",
    ],
)
