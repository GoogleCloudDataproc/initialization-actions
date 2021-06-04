package(default_visibility = ["//visibility:public"])
exports_files(["llap.sh","start_llap.sh"])

py_test(
    name = "test_hive_llap",
    size = "large",
    srcs = ["test_hive_llap.py","run_hive_commands.py"],
    data = ["llap.sh","start_llap.sh"],
    local = True,
    shard_count = 6,
    deps = [
        "//integration_tests:dataproc_test_case",
        "@io_abseil_py//absl/testing:parameterized",
    ],
)