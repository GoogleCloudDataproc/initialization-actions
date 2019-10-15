package(default_visibility = ["//visibility:public"])

py_test(
	name = "test_tez",
	srcs = ["test_tez.py"],
	deps = ["//integration_tests:dataproc_test_case",
			":verify_tez",
			"@io_abseil_py//absl/flags"],
	data = ["tez.sh"],
	local = True,
	size = "enormous",
)

py_library(
	name = "verify_tez",
	srcs = ["verify_tez.py"],
)