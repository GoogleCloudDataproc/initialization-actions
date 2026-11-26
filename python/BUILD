package(default_visibility = ["//visibility:public"])


py_test(
    name = "test_pip_cached",
    size = "enormous",
    srcs = ["test_pip_cached.py"],
    data = [
        "pip-install-cached.sh",
    ],
    local = True,
    deps = [
        "//integration_tests:dataproc_test_case",
        "@io_abseil_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "test_conda_cached",
    size = "enormous",
    srcs = ["test_conda_cached.py"],
    data = [
        "conda-install-cached.sh",
    ],
    local = True,
    deps = [
        "//integration_tests:dataproc_test_case",
        "@io_abseil_py//absl/testing:parameterized",
    ],
)

test_suite(
    name = "test_python",
    tests = [
        ":test_pip_cached",
        ":test_conda_cached",
    ],
)
