package(default_visibility = ["//visibility:public"])

py_test(
	name = "test_rapids",
	srcs = ["test_rapids.py"],
	deps = ["//integration_tests:dataproc_test_case",
			":verify_rapids",
			"@io_abseil_py//absl/flags"],
	data = ["rapids.sh"],
	local = True,
	size = "enormous",
)

py_library(
	name = "verify_rapids",
	srcs = ["verify_rapids.py"],
)