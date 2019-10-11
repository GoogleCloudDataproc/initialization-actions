package(default_visibility = ["//visibility:public"])

py_test(
	name = "test_oozie",
	srcs = ["test_oozie.py"],
	deps = ["//integration_tests:dataproc_test_case",
			"//absl/flags",],
	data = ["oozie.sh",
			"validate.sh"],
	local = True,
	size = "enormous",
)