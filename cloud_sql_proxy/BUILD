package(default_visibility = ["//visibility:public"])

py_test(
	name = "test_cloud_sql",
	srcs = ["test_cloud_sql.py"],
	deps = ["//integration_tests:dataproc_test_case",
			"//absl/flags",
			":pyspark_metastore_test"],
	data = ["cloud-sql-proxy.sh"],
	local = True,
	size = "enormous",
)

py_library(
	name = "pyspark_metastore_test",
	srcs = ["pyspark_metastore_test.py"],
)