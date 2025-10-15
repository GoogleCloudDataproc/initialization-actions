package(default_visibility = ["//visibility:public"])

exports_files(["install_gpu_driver.sh", "mig.sh"])

py_library(
    name = "gpu_test_case_base",
    srcs = ["gpu_test_case_base.py"],
    srcs_version = "PY3",
    testonly = True,  # Add this line
    deps = [
        "//integration_tests:dataproc_test_case",
        "@io_abseil_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "test_gpu",
    size = "enormous",
    srcs = ["test_gpu.py"],
    data = ["install_gpu_driver.sh", "mig.sh"],
    local = True,
    shard_count = 15,
    deps = [
        ":gpu_test_case_base",  # Add this dependency
        "//integration_tests:dataproc_test_case",
        "@io_abseil_py//absl/testing:parameterized",
    ],
)