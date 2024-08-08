package(default_visibility = ["//visibility:public"])

exports_files(["kafka.sh"])

py_test(
    name = "test_kafka",
    size = "enormous",
    srcs = ["test_kafka.py"],
    data = [
        "kafka.sh",
        "validate.sh",
        "cruise-control.sh",
        "kafka-manager.sh",
    ],
    local = True,
    shard_count = 9,
    deps = [
        ":verify_kafka_running",
        "//integration_tests:dataproc_test_case",
        "@io_abseil_py//absl/testing:parameterized",
    ],
)

py_library(
    name = "verify_kafka_running",
    testonly = True,
    srcs = ["verify_kafka_running.py"],
)
