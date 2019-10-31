package(default_visibility = ["//visibility:public"])

exports_files(["presto.sh"])

py_test(
    name = "test_presto",
    size = "enormous",
    srcs = ["test_presto.py"],
    data = ["presto.sh"],
    local = True,
    shard_count = 4,
    deps = [
        "//integration_tests:dataproc_test_case",
        "@io_abseil_py//absl/testing:parameterized",
    ],
)
