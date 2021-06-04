package(default_visibility = ["//visibility:public"])

py_test(
    name = "test_hive_llap",
    size = "large",
    srcs = ["//hive-llap/test_hive_llap.py","//hive-llap/run_hive_commands.py"],
    data = ["//hive-llap/llap.sh","//hive-llap/start_llap.sh"],
    local = True,
    shard_count = 6,
    deps = [
        "//integration_tests:dataproc_test_case",
        "@io_abseil_py//absl/testing:parameterized",
    ],
) 