package(default_visibility = ["//visibility:public"])

exports_files(["horovod.sh"])

py_test(
    name = "test_horovod",
    size = "enormous",
    srcs = ["test_horovod.py"],
    data = [
        "horovod.sh",
        "//gpu:install_gpu_driver.sh",
        "scripts/verify_mxnet.py",
        "scripts/verify_pytorch.py",
        "scripts/verify_tensorflow.py",
    ],
    local = True,
    shard_count = 4,
    deps = [
        "//integration_tests:dataproc_test_case",
        "@io_abseil_py//absl/testing:parameterized",
    ],
)
