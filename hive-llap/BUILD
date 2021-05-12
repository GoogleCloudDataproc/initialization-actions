package(default_visibility = ["//visibility:public"])

py_test(
    name = "test_llap",
    size = "enormous",
    srcs = ["test_llap.py","run_hive_commands.py"],
    data = ["llap.sh"],
    local = True,
    shard_count = 4,
    deps = [
        "//integration_tests:dataproc_test_case",
        "@io_abseil_py//absl/testing:parameterized",
    ],
)