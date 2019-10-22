package(default_visibility = ["//visibility:public"])

py_test(
    name = "test_hue",
    size = "enormous",
    srcs = ["test_hue.py"],
    data = ["hue.sh"],
    local = True,
    deps = ["//integration_tests:dataproc_test_case"],
)
