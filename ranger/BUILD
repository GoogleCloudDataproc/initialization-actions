package(default_visibility = ["//visibility:public"])

py_test(
    name = "test_ranger",
    size = "enormous",
    srcs = ["test_ranger.py"],
    data = [
        "ranger.sh",
        "//solr:solr.sh",
    ],
    local = True,
    deps = [
        ":verify_ranger",
        "//integration_tests:dataproc_test_case",
    ],
)

py_library(
    name = "verify_ranger",
    testonly = True,
    srcs = ["verify_ranger.py"],
)
