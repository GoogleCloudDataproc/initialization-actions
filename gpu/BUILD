package(default_visibility = ["//visibility:public"])

py_test(
    name = "test_gpu",
    size = "enormous",
    srcs = ["test_gpu.py"],
    data = ["install_gpu_driver.sh"],
    local = True,
    deps = ["//integration_tests:dataproc_test_case"],
)
