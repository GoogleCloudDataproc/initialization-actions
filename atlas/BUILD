package(default_visibility = ["//visibility:public"])

py_test(
    name = "test_atlas",
    size = "enormous",
    srcs = ["test_atlas.py"],
    data = [
        "atlas.sh",
        "populate_atlas.sh",
        "//kafka:kafka.sh",
    ],
    local = True,
    shard_count = 7,
    deps = [
        ":validate_atlas",
        "//integration_tests:dataproc_test_case",
        "@io_abseil_py//absl/testing:parameterized",
    ],
)

py_library(
    name = "validate_atlas",
    testonly = True,
    srcs = ["validate_atlas.py"],
)
