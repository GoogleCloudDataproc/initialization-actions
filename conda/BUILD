package(default_visibility = ["//visibility:public"])

exports_files(["bootstrap-conda.sh"])

py_test(
    name = "test_conda",
    size = "enormous",
    srcs = ["test_conda.py"],
    data = [
        "bootstrap-conda.sh",
        "install-conda-env.sh",
    ],
    local = True,
    shard_count = 2,
    deps = [
        ":get_sys_exec",
        "//integration_tests:dataproc_test_case",
        "@io_abseil_py//absl/testing:parameterized",
    ],
)

py_library(
    name = "get_sys_exec",
    testonly = True,
    srcs = ["get-sys-exec.py"],
)
