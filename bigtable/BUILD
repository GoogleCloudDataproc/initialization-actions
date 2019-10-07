package(default_visibility = ["//visibility:public"])

py_test(
	name = "test_bigtable",
	srcs = ["test_bigtable.py"],
	args = ["--params='SINGLE 1.2 m'"],
	deps = ["//integration_tests:dataproc_test_case",
			":run_hbase_commands",
			"//absl/flags",],
	data = ["bigtable.sh"],
	local = True,
)

py_library(
	name = "run_hbase_commands",
	srcs = ["run_hbase_commands.py"],
)