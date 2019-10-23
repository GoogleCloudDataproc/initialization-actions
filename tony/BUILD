package(default_visibility = ["//visibility:public"])

py_test(
    name = "test_tony",
    size = "enormous",
    srcs = ["test_tony.py"],
    data = ["tony.sh"],
    local = True,
    deps = [
        "//integration_tests:dataproc_test_case",
        "@io_abseil_py//absl/testing:parameterized",
    ],
)
