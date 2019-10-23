package(default_visibility = ["//visibility:public"])

py_test(
    name = "test_tez",
    size = "enormous",
    srcs = ["test_tez.py"],
    data = ["tez.sh"],
    local = True,
    shard_count = 3,
    deps = [
        ":verify_tez",
        "//integration_tests:dataproc_test_case",
        "@io_abseil_py//absl/testing:parameterized",
    ],
)

py_library(
    name = "verify_tez",
    testonly = True,
    srcs = ["verify_tez.py"],
)
