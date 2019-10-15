package(default_visibility = ["//visibility:public"])

py_test(
	name = "test_flink",
	srcs = ["test_flink.py"],
	deps = ["//integration_tests:dataproc_test_case",
			"@io_abseil_py//absl/flags"],
	data = ["flink.sh",
			"validate.sh"],
	local = True,
	size = "enormous",
)
