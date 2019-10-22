package(default_visibility = ["//visibility:public"])

py_test(
    name = "test_datalab",
    size = "enormous",
    srcs = ["test_datalab.py"],
    data = ["datalab.sh"],
    local = True,
    deps = ["//integration_tests:dataproc_test_case"],
)
