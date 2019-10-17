package(default_visibility = ["//visibility:public"])

py_test(
	name = "test_hbase",
	srcs = ["test_hbase.py"],
	deps = ["//integration_tests:dataproc_test_case"],
	data = ["hbase.sh"],
	local = True,
	size = "enormous",
)