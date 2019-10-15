package(default_visibility = ["//visibility:public"])

py_test(
	name = "test_jupyter",
	srcs = ["test_jupyter.py"],
	deps = ["//integration_tests:dataproc_test_case",
			"@io_abseil_py//absl/flags"],
	data = ["jupyter.sh",
			"launch-jupyter-interface.sh"],
	local = True,
	size = "enormous",
)
