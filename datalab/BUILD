package(default_visibility = ["//visibility:public"])

py_test(
	name = "test_datalab",
	srcs = ["test_datalab.py"],
	deps = ["//integration_tests:dataproc_test_case",
			"//absl:app",
			"//absl/flags"],
	data = ["datalab.sh"],
	local = True,
	size = "enormous",
)