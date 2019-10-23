package(default_visibility = ["//visibility:public"])

py_test(
    name = "test_rstudio",
    size = "enormous",
    srcs = ["test_rstudio.py"],
    data = ["rstudio.sh"],
    local = True,
    shard_count = 4,
    deps = [
        "//integration_tests:dataproc_test_case",
        "@io_abseil_py//absl/testing:parameterized",
    ],
)
