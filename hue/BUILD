package(default_visibility = ["//visibility:public"])

py_test(
    name = "test_hue",
    size = "enormous",
    srcs = ["test_hue.py"],
    data = ["hue.sh"],
    local = True,
    shard_count = 3,
    deps = [
        "//integration_tests:dataproc_test_case",
        "@io_abseil_py//absl/testing:parameterized",
    ],
)
