package(default_visibility = ["//visibility:public"])

py_test(
    name = "test_livy",
    size = "enormous",
    srcs = ["test_livy.py"],
    data = ["livy.sh"],
    local = True,
    deps = [
        ":verify_livy_running",
        "//integration_tests:dataproc_test_case",
    ],
)

py_library(
    name = "verify_livy_running",
    srcs = ["verify_livy_running.py"],
)
