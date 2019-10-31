package(default_visibility = ["//visibility:public"])

py_test(
    name = "test_alluxio",
    size = "enormous",
    srcs = ["test_alluxio.py"],
    data = [
        "alluxio.sh",
        "//presto:presto.sh",
    ],
    local = True,
    shard_count = 2,
    deps = [
        "//integration_tests:dataproc_test_case",
        "@io_abseil_py//absl/testing:parameterized",
    ],
)
