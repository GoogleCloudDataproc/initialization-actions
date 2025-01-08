package(default_visibility = ["//visibility:public"])

py_test(
    name = "test_ganglia",
    size = "enormous",
    srcs = ["test_ganglia.py"],
    data = [
        "ganglia.sh",
	"verify_ganglia_running.pl",
    ],
    local = True,
    shard_count = 4,
    deps = [
        "//integration_tests:dataproc_test_case",
        "@io_abseil_py//absl/testing:parameterized",
    ],
)
