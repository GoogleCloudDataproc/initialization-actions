package(default_visibility = ["//visibility:public"])

py_test(
    name = "test_spark_bigquery_connector",
    size = "enormous",
    srcs = ["test_spark_bigquery_connector.py"],
    data = [
        "install_connector.sh",
    ],
    local = True,
    shard_count = 3,
    deps = [
        "//integration_tests:dataproc_test_case",
        "@io_abseil_py//absl/testing:parameterized",
    ],
)