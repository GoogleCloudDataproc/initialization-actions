package(default_visibility = ["//visibility:public"])

py_test(
    name = "test_open_telemetry_agent",
    size = "enormous",
    srcs = ["otel/test_otel.py"],
    data = ["otel/otel.sh"],
    local = True,
    shard_count = 3,
    deps = [
        "//integration_tests:dataproc_test_case",
        "@io_abseil_py//absl/testing:parameterized",
    ],
)