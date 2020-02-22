package(default_visibility = ["//visibility:public"])

py_test(
    name = "test_h2o",
    size = "enormous",
    srcs = ["test_h2o.py"],
    data = [
        "h2o.sh",
        "sample-script.py",
        "//conda:bootstrap-conda.sh",
    ],
    local = True,
    shard_count = 9,
    deps = [
        "//integration_tests:dataproc_test_case",
        "@io_abseil_py//absl/testing:parameterized",
    ],
)
