package(default_visibility = ["//visibility:public"])

py_test(
	name = "test_kafka",
	srcs = ["test_kafka.py"],
	deps = ["//integration_tests:dataproc_test_case",
			"//absl/flags",],
	data = ["kafka.sh",
			"validate.sh"],
	local = True,
	size = "enormous",
)