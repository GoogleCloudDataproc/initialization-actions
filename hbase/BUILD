package(default_visibility = ["//visibility:public"])

py_test(
    name = "test_hbase",
    size = "enormous",
    srcs = ["test_hbase.py"],
    data = ["hbase.sh"],
    local = True,
    deps = ["//integration_tests:dataproc_test_case"],
)
