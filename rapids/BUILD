package(default_visibility = ["//visibility:public"])

py_test(
    name = "test_rapids",
    size = "enormous",
    srcs = ["test_rapids.py"],
    data = ["rapids.sh"],
    local = True,
    deps = [
        ":verify_rapids",
        "//integration_tests:dataproc_test_case",
    ],
)

py_library(
    name = "verify_rapids",
    srcs = ["verify_rapids.py"],
)
