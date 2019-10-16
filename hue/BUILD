package(default_visibility = ["//visibility:public"])

py_test(
	name = "test_hue",
	srcs = ["test_hue.py"],
	deps = ["//integration_tests:dataproc_test_case",
			"@io_abseil_py//absl/flags"],
	data = ["hue.sh"],
	local = True,
	size = "enormous",
)