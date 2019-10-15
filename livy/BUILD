package(default_visibility = ["//visibility:public"])

py_test(
	name = "test_livy",
	srcs = ["test_livy.py"],
	deps = ["//integration_tests:dataproc_test_case",
			":verify_livy_running",
			"@io_abseil_py//absl/flags"],
	data = ["livy.sh"],
	local = True,
	size = "enormous",
)

py_library(
	name = "verify_livy_running",
	srcs = ["verify_livy_running.py"],
)
