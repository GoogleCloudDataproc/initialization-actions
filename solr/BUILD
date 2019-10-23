package(default_visibility = ["//visibility:public"])

exports_files(["solr.sh"])

py_test(
    name = "test_solr",
    size = "enormous",
    srcs = ["test_solr.py"],
    data = ["solr.sh"],
    local = True,
    shard_count = 3,
    deps = [
        ":verify_solr",
        "//integration_tests:dataproc_test_case",
        "@io_abseil_py//absl/testing:parameterized",
    ],
)

py_library(
    name = "verify_solr",
    testonly = True,
    srcs = ["verify_solr.py"],
)
