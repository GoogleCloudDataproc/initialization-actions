package(default_visibility = ["//visibility:public"])

py_test(
    name = "test_conda",
    size = "enormous",
    srcs = ["test_conda.py"],
    data = [
        "bootstrap-conda.sh",
        "install-conda-env.sh",
    ],
    local = True,
    deps = [
        ":get_sys_exec",
        "//integration_tests:dataproc_test_case",
    ],
)

py_library(
    name = "get_sys_exec",
    srcs = ["get-sys-exec.py"],
)
