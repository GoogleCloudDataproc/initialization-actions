package(default_visibility = ["//visibility:public"])

exports_files(["trino.sh"])

py_test(
    name = "test_trino",
    size = "enormous",
    srcs = ["test_trino.py"],
    data = ["trino.sh"],
    local = True,
    shard_count = 4,
    deps = [
        "//integration_tests:dataproc_test_case",
        "@io_abseil_py//absl/testing:parameterized",
    ],
)
