package(default_visibility = ["//visibility:public"])

test_suite(
    name = "DataprocInitActionsTestSuite",
    tests = [
        ":test_cloud_sql_proxy",
        ":test_dr_elephant",
        ":test_hive_hcatalog",
        ":test_starburst_presto",
        "//alluxio:test_alluxio",
        "//atlas:test_atlas",
        "//bigtable:test_bigtable",
        "//conda:test_conda",
        "//connectors:test_connectors",
        "//datalab:test_datalab",
        "//drill:test_drill",
        "//flink:test_flink",
        "//ganglia:test_ganglia",
        "//gpu:test_gpu",
        "//h2o:test_h2o",
        "//hbase:test_hbase",
        "//hue:test_hue",
        "//jupyter:test_jupyter",
        "//jupyter_sparkmonitor:test_jupyter_sparkmonitor",
        "//kafka:test_kafka",
        "//knox:test_knox",
        "//livy:test_livy",
        "//mlvm:test_mlvm",
        "//oozie:test_oozie",
        "//presto:test_presto",
        "//ranger:test_ranger",
        "//rapids:test_rapids",
        "//rstudio:test_rstudio",
        "//solr:test_solr",
        "//sqoop:test_sqoop",
        "//tez:test_tez",
        "//tony:test_tony",
    ],
)

py_test(
    name = "test_cloud_sql_proxy",
    size = "enormous",
    srcs = ["cloud-sql-proxy/test_cloud_sql_proxy.py"],
    data = ["cloud-sql-proxy/cloud-sql-proxy.sh"],
    local = True,
    shard_count = 3,
    deps = [
        ":pyspark_metastore_test",
        "//integration_tests:dataproc_test_case",
        "@io_abseil_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "test_dr_elephant",
    size = "enormous",
    srcs = ["dr-elephant/test_dr_elephant.py"],
    data = ["dr-elephant/dr-elephant.sh"],
    local = True,
    shard_count = 2,
    deps = [
        "//integration_tests:dataproc_test_case",
        "@io_abseil_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "test_hive_hcatalog",
    size = "enormous",
    srcs = ["hive-hcatalog/test_hive_hcatalog.py"],
    data = ["hive-hcatalog/hive-hcatalog.sh"],
    local = True,
    shard_count = 6,
    deps = [
        "//integration_tests:dataproc_test_case",
        "@io_abseil_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "test_starburst_presto",
    size = "enormous",
    srcs = ["starburst-presto/test_starburst_presto.py"],
    data = ["starburst-presto/presto.sh"],
    local = True,
    shard_count = 4,
    deps = [
        "//integration_tests:dataproc_test_case",
        "@io_abseil_py//absl/testing:parameterized",
    ],
)

py_library(
    name = "pyspark_metastore_test",
    testonly = True,
    srcs = ["cloud-sql-proxy/pyspark_metastore_test.py"],
)
