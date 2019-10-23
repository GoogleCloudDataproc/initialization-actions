package(default_visibility = ["//visibility:public"])

py_test(
    name = "test_bigtable",
    size = "enormous",
    srcs = ["test_bigtable.py"],
    data = ["bigtable.sh"],
    local = True,
    shard_count = 3,
    deps = [
        ":run_hbase_commands",
        "//integration_tests:dataproc_test_case",
        "@io_abseil_py//absl/testing:parameterized",
    ],
)

py_library(
    name = "run_hbase_commands",
    srcs = ["run_hbase_commands.py"],
)
