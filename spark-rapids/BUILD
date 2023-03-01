package(default_visibility = ["//visibility:public"])

exports_files(["spark-rapids.sh", "mig.sh"])

py_test(
    name = "test_spark_rapids",
    size = "enormous",
    srcs = ["test_spark_rapids.py"],
    data = ["spark-rapids.sh", "mig.sh"],
    local = True,
    shard_count = 15,
    deps = [
        "//integration_tests:dataproc_test_case",
        "@io_abseil_py//absl/testing:parameterized",
    ],
)
