package(default_visibility = ["//visibility:public"])

py_test(
    name = "test_atlas",
    size = "enormous",
    srcs = ["test_atlas.py"],
    data = ["atlas.sh"],
    local = True,
    shard_count = 2,
    deps = [
        "//integration_tests:dataproc_test_case",
        "@io_abseil_py//absl/testing:parameterized",
    ],
)