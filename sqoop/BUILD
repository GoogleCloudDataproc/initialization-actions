package(default_visibility = ["//visibility:public"])

py_test(
    name = "test_sqoop",
    size = "enormous",
    srcs = ["test_sqoop.py"],
    data = ["sqoop.sh", "test_sql_db_dump.gz", "sqoop_sql.sh"],
    local = True,
    shard_count = 6,
    deps = [
        "//integration_tests:dataproc_test_case",
        "@io_abseil_py//absl/testing:parameterized",
    ],
)
