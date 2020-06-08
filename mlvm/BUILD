py_test(
    name = "test_mlvm",
    size = "enormous",
    srcs = ["test_mlvm.py"],
    data = ["mlvm.sh"],
    local = True,
    shard_count = 10,
    deps = [
        "//integration_tests:dataproc_test_case",
        "@io_abseil_py//absl/testing:parameterized",
    ],
)
