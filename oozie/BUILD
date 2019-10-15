package(default_visibility = ["//visibility:public"])

py_test(
	name = "test_oozie",
	srcs = ["test_oozie.py"],
	deps = ["//integration_tests:dataproc_test_case",
			"@io_abseil_py//absl/flags"],
	data = ["oozie.sh",
			"validate.sh"],
	local = True,
	size = "enormous",
)