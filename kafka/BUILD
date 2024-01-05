package(default_visibility = ["//visibility:public"])

exports_files(["kafka.sh"])

py_test(
    name = "test_kafka",
    size = "enormous",
    srcs = ["test_kafka.py"],
    data = [
        "kafka.sh",
        "validate.sh",
        "kafka_python.sh",
        "kafka_python3.sh",
    ],
    local = True,
    shard_count = 1,
    deps = [
        ":verify_kafka_running.py",
        ":verify_kafka3_running_py3.py",
        "//integration_tests:dataproc_test_case",
        "@io_abseil_py//absl/testing:parameterized",
    ],
)
