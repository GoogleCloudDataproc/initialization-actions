package(default_visibility = ["//visibility:public"])

test_suite(
    name = "DataprocInitActionsTestSuite",
    tests = [
        ":test_cloud_sql_proxy",
        ":test_dr_elephant",
        ":test_hive_hcatalog",
        ":test_hive_llap",
        ":test_starburst_presto",
        ":test_spark_rapids",
        "//alluxio:test_alluxio",
        "//atlas:test_atlas",
        "//bigtable:test_bigtable",
        "//conda:test_conda",
        "//connectors:test_connectors",
        "//dask:test_dask",
        "//drill:test_drill",
        "//flink:test_flink",
        "//ganglia:test_ganglia",
        "//gpu:test_gpu",
        "//h2o:test_h2o",
        "//hbase:test_hbase",
        "//horovod:test_horovod",
        "//hue:test_hue",
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
        "//tony:test_tony",
    ],
)

py_test(
    name = "test_hive_llap",
    size = "large",
    srcs = [
        "hive-llap/run_hive_commands.py",
        "hive-llap/test_hive_llap.py",
    ],
    data = [
        "hive-llap/llap.sh",
        "hive-llap/start_llap.sh",
    ],
    local = True,
    shard_count = 6,
    deps = [
        "//integration_tests:dataproc_test_case",
        "@io_abseil_py//absl/testing:parameterized",
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

py_test(
    name = "test_spark_rapids",
    size = "enormous",
    srcs = ["spark-rapids/test_spark_rapids.py"],
    data = [
        "spark-rapids/spark-rapids.sh",
        "spark-rapids/verify_xgboost_spark_rapids.scala",
        "spark-rapids/mig.sh",
    ],
    local = True,
    shard_count = 3,
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
