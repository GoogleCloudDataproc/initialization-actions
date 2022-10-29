package(default_visibility = ["//visibility:public"])

exports_files(["spark-rapids.sh"])

py_test(
    name = "test_sparkRapids",
    size = "enormous",
    srcs = ["test_sparkRapids.py"],
    data = [
        "spark-rapids.sh",
        "verify_xgboost_spark_rapids.scala",
    ],
    local = True,
    shard_count = 3,
    deps = [
        "//integration_tests:dataproc_test_case",
        "@io_abseil_py//absl/testing:parameterized",
    ],
)

