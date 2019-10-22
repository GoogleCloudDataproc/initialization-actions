package(default_visibility = ["//visibility:public"])

py_test(
    name = "test_tez",
    size = "enormous",
    srcs = ["test_tez.py"],
    data = ["tez.sh"],
    local = True,
    deps = [
        ":verify_tez",
        "//integration_tests:dataproc_test_case",
        "@io_abseil_py//absl/flags",
    ],
)

py_library(
    name = "verify_tez",
    srcs = ["verify_tez.py"],
)
