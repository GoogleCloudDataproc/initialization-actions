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
    shard_count = 3,
    deps = [
        ":verify_ranger",
        "//integration_tests:dataproc_test_case",
        "@io_abseil_py//absl/testing:parameterized",
    ],
)

py_library(
    name = "verify_ranger",
    testonly = True,
    srcs = ["verify_ranger.py"],
)
