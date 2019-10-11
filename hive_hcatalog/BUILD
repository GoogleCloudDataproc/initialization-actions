package(default_visibility = ["//visibility:public"])

py_test(
	name = "test_hive",
	srcs = ["test_hive.py"],
	deps = ["//integration_tests:dataproc_test_case",
			"//absl/flags",],
	data = ["hive-hcatalog.sh"],
	local = True,
	size = "enormous",
)