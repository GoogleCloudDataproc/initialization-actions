package(default_visibility = ["//visibility:public"])

py_test(
	name = "test_cloud_sql_proxy",
	srcs = ["cloud-sql-proxy/test_cloud_sql_proxy.py"],
	deps = ["//integration_tests:dataproc_test_case",
			":pyspark_metastore_test"],
	data = ["cloud-sql-proxy/cloud-sql-proxy.sh"],
	local = True,
	size = "enormous",
)

py_library(
	name = "pyspark_metastore_test",
	srcs = ["cloud-sql-proxy/pyspark_metastore_test.py"],
)

py_test(
	name = "test_starburst_presto",
	srcs = ["starburst-presto/test_starburst_presto.py"],
	deps = ["//integration_tests:dataproc_test_case"],
	data = ["starburst-presto/presto.sh"],
	local = True,
	size = "enormous",
)

py_test(
	name = "test_hive_hcatalog",
	srcs = ["hive-hcatalog/test_hive_hcatalog.py"],
	deps = ["//integration_tests:dataproc_test_case"],
	data = ["hive-hcatalog/hive-hcatalog.sh"],
	local = True,
	size = "enormous",
)

test_suite(
	name = "DataprocInitActionsTestSuite",
	tests = ["//bigtable:test_bigtable",
			 ":test_cloud_sql_proxy",
			 "//conda:test_conda",
			 "//datalab:test_datalab",
			 "//drill:test_drill",
			 "//flink:test_flink",
			 "//ganglia:test_ganglia",
			 "//gpu:test_gpu",
			 "//hbase:test_hbase",
			 ":test_hive_hcatalog",
			 "//hue:test_hue",
			 "//jupyter:test_jupyter",
			 "//kafka:test_kafka",
			 "//livy:test_livy",
			 "//oozie:test_oozie",
			 "//presto:test_presto",
			 "//ranger:test_ranger",
			 "//rapids:test_rapids",
			 "//rstudio:test_rstudio",
			 "//solr:test_solr",
			 ":test_starburst_presto",
			 "//tez:test_tez",
			 "//tony:test_tony"]
)