package(default_visibility = ["//visibility:public"])

py_test(
    name = "test_livy",
    size = "enormous",
    srcs = ["test_livy.py"],
    data = ["livy.sh"],
    local = True,
    shard_count = 3,
    deps = [
        ":verify_livy_running",
        "//integration_tests:dataproc_test_case",
        "@io_abseil_py//absl/testing:parameterized",
    ],
)

py_library(
    name = "verify_livy_running",
    testonly = True,
    srcs = ["verify_livy_running.py"],
)
