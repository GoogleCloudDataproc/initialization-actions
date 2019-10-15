package(default_visibility = ["//visibility:public"])

py_test(
	name = "test_rstudio",
	srcs = ["test_rstudio.py"],
	deps = ["//integration_tests:dataproc_test_case",
			"@io_abseil_py//absl/flags"],
	data = ["rstudio.sh"],
	local = True,
	size = "enormous",
)