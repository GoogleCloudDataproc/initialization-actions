package(default_visibility = ["//visibility:public"])

py_test(
    name = "test_jupyter",
    size = "enormous",
    srcs = ["test_jupyter.py"],
    data = [
        "jupyter.sh",
        "launch-jupyter-interface.sh",
    ],
    local = True,
    deps = ["//integration_tests:dataproc_test_case"],
)
