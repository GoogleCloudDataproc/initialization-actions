package(default_visibility = ["//visibility:public"])

py_test(
    name = "test_presto",
    size = "enormous",
    srcs = ["test_presto.py"],
    data = ["presto.sh"],
    local = True,
    deps = ["//integration_tests:dataproc_test_case"],
)
