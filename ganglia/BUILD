package(default_visibility = ["//visibility:public"])

py_test(
    name = "test_ganglia",
    size = "enormous",
    srcs = ["test_ganglia.py"],
    local = True,
    deps = [
        ":verify_ganglia_running",
        "//integration_tests:dataproc_test_case",
    ],
)

py_library(
    name = "verify_ganglia_running",
    srcs = ["verify_ganglia_running.py"],
)
