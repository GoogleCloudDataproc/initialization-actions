package(default_visibility = ["//visibility:public"])

py_test(
    name = "test_bigtable",
    size = "enormous",
    srcs = ["test_bigtable.py"],
    data = ["bigtable.sh"],
    local = True,
    deps = [
        ":run_hbase_commands",
        "//integration_tests:dataproc_test_case",
    ],
)

py_library(
    name = "run_hbase_commands",
    srcs = ["run_hbase_commands.py"],
)
