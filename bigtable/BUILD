package(default_visibility = ["//visibility:public"])

py_test(
	name = "test_bigtable",
	srcs = ["test_bigtable.py"],
	deps = ["//integration_tests:dataproc_test_case",
			":run_hbase_commands",
			"@io_abseil_py//absl/flags"],
	data = ["bigtable.sh"],
	local = True,
	size = "enormous",
)

py_library(
	name = "run_hbase_commands",
	srcs = ["run_hbase_commands.py"],
)