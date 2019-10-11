package(default_visibility = ["//visibility:public"])

py_test(
	name = "test_install_gpu_driver",
	srcs = ["test_install_gpu_driver.py"],
	deps = ["//integration_tests:dataproc_test_case",
			"//absl/flags",],
	data = ["install_gpu_driver.sh"],
	local = True,
	size = "enormous",
)