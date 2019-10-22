package(default_visibility = ["//visibility:public"])

py_test(
    name = "test_drill",
    size = "enormous",
    srcs = ["test_drill.py"],
    data = [
        "drill.sh",
        "validate.sh",
        "//zookeeper:zookeeper.sh",
    ],
    local = True,
    deps = ["//integration_tests:dataproc_test_case"],
)
