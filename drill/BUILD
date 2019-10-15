package(default_visibility = ["//visibility:public"])

py_test(
	name = "test_drill",
	srcs = ["test_drill.py"],
	deps = ["//integration_tests:dataproc_test_case",
			"@io_abseil_py//absl/flags"],
	data = ["drill.sh",
			"validate.sh",
			"//zookeeper:zookeeper.sh"],
	local = True,
	size = "enormous",
)
