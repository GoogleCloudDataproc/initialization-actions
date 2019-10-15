package(default_visibility = ["//visibility:public"])

py_test(
	name = "test_hbase",
	srcs = ["test_hbase.py"],
	deps = ["//integration_tests:dataproc_test_case",
			"@io_abseil_py//absl/flags"],
	data = ["hbase.sh"],
	local = True,
	size = "enormous",
)