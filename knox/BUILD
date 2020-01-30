package(default_visibility = ["//visibility:public"])

exports_files(["knox.sh"])

py_test(
    name = "test_knox",
    size = "enormous",
    srcs = ["test_knox.py"],
    data = [
        "knox.sh",
        "verify_knox.sh",
    ],
    local = True,
    shard_count = 2,
    deps = [
        "//integration_tests:dataproc_test_case",
        "@io_abseil_py//absl/testing:parameterized",
    ],
)
