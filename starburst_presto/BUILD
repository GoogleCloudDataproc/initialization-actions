package(default_visibility = ["//visibility:public"])

py_test(
	name = "test_presto",
	srcs = ["test_presto.py"],
	deps = ["//integration_tests:dataproc_test_case",
			"//absl/flags",],
	data = ["presto.sh"],
	local = True,
	size = "enormous",
)
