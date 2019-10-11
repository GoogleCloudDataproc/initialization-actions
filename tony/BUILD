package(default_visibility = ["//visibility:public"])

py_test(
	name = "test_tony",
	srcs = ["test_tony.py"],
	deps = ["//integration_tests:dataproc_test_case",
			"//absl/flags",],
	data = ["tony.sh"],
	local = True,
	size = "enormous",
)