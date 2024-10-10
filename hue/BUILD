package(default_visibility = ["//visibility:public"])

exports_files(["hue.sh"])

py_test(
    name = "test_hue",
    size = "enormous",
    srcs = ["test_hue.py",
            "verify_hue_running.py",
            "run_queries.py"],
    data = ["hue.sh"],
    local = True,
    shard_count = 7,
    deps = [
        "//integration_tests:dataproc_test_case",
        "@io_abseil_py//absl/testing:parameterized",
    ],
)
