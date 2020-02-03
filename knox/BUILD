package(default_visibility = ["//visibility:public"])

exports_files(["knox.sh"])

py_test(
    name = "test_knox",
    size = "enormous",
    srcs = ["test_knox.py"],
    data = [
        "gateway-site.xml",
        "knox.sh",
        "knox-config.yaml",
        "verify_knox.sh",
    ] + glob([
        "services/*",
        "topologies/*",
    ]),
    local = True,
    shard_count = 6,
    deps = [
        "//integration_tests:dataproc_test_case",
        "@io_abseil_py//absl/testing:parameterized",
    ],
)
