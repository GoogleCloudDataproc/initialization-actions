package(default_visibility = ["//visibility:public"])

py_test(
    name = "test_llap",
    size = "large",
    srcs = ["test_llap.py","run_hive_commands.py"],
    data = ["llap.sh","start_llap.sh"],
    local = True,
    shard_count = 6,
    deps = [
        "//integration_tests:dataproc_test_case",
        "@io_abseil_py//absl/testing:parameterized",
    ],
)