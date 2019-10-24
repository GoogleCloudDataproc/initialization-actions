package(default_visibility = ["//visibility:public"])

py_test(
    name = "test_datalab",
    size = "enormous",
    srcs = ["test_datalab.py"],
    data = [
        "datalab.sh",
        "//conda:bootstrap-conda.sh",
        "//docker:docker.sh",
    ],
    local = True,
    shard_count = 2,
    deps = [
        "//integration_tests:dataproc_test_case",
        "@io_abseil_py//absl/testing:parameterized",
    ],
)
