package(default_visibility = ["//visibility:public"])

py_test(
    name = "test_ganglia",
    size = "enormous",
    srcs = ["test_ganglia.py"],
    data = [
        "ganglia.sh",
    ],
    local = True,
    shard_count = 3,
    deps = [
        ":verify_ganglia_running",
        "//integration_tests:dataproc_test_case",
        "@io_abseil_py//absl/testing:parameterized",
    ],
)

py_library(
    name = "verify_ganglia_running",
    srcs = ["verify_ganglia_running.py"],
)
