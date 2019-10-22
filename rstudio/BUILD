package(default_visibility = ["//visibility:public"])

py_test(
    name = "test_rstudio",
    size = "enormous",
    srcs = ["test_rstudio.py"],
    data = ["rstudio.sh"],
    local = True,
    deps = ["//integration_tests:dataproc_test_case"],
)
