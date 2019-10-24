package(default_visibility = ["//visibility:public"])

py_test(
    name = "test_jupyter",
    size = "enormous",
    srcs = ["test_jupyter.py"],
    data = [
        "jupyter.sh",
        "launch-jupyter-interface.sh",
        "//conda:bootstrap-conda.sh",
        "//util:utils.sh",
    ] + glob([
        "internal/*",
        "kernels/*",
    ]),
    local = True,
    shard_count = 4,
    deps = [
        "//integration_tests:dataproc_test_case",
        "@io_abseil_py//absl/testing:parameterized",
    ],
)
