package(default_visibility = ["//visibility:public"])

py_test(
	name = "test_hue",
	srcs = ["test_hue.py"],
	deps = ["//integration_tests:dataproc_test_case"],
	data = ["hue.sh"],
	local = True,
	size = "enormous",
)