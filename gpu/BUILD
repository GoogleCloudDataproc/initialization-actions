package(default_visibility = ["//visibility:public"])

py_test(
	name = "test_gpu",
	srcs = ["test_gpu.py"],
	deps = ["//integration_tests:dataproc_test_case"],
	data = ["install_gpu_driver.sh"],
	local = True,
	size = "enormous",
)