package(default_visibility = ["//visibility:public"])

py_test(
    name = "test_hbase",
    size = "enormous",
    srcs = ["test_hbase.py"],
    data = [
        "hbase.sh",
        "//zookeeper:zookeeper.sh",
    ],
    local = True,
    shard_count = 6,
    deps = [
        "//integration_tests:dataproc_test_case",
        "@io_abseil_py//absl/testing:parameterized",
    ],
)
