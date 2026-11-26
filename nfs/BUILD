# nfs/BUILD
load("@rules_python//python:test.bzl", "py_test")

package(default_visibility = ["//visibility:public"])

py_test(
    name = "test_nfs",
    size = "large",
    srcs = ["test_nfs.py"],
    data = [
        "nfs.sh",
    ],
    main = "test_nfs.py",
    python_version = "PY3",
    shard_count = 2,  # One for standard, one for Kerberos
    tags = [
        "external",
        "init_action",
        "nfs",
    ],
    deps = [
        "//integration_tests:dataproc_test_case",
        "@io_abseil_py//absl/testing:absltest",
    ],
)

filegroup(
    name = "nfs_init_action",
    srcs = ["nfs.sh"],
)
