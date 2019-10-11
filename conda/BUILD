package(default_visibility = ["//visibility:public"])

py_test(
	name = "test_conda",
	srcs = ["test_conda.py"],
	deps = ["//integration_tests:dataproc_test_case",
			"//absl/flags",
			":get_sys_exec"],
	data = ["install-conda-env.sh",
			"bootstrap-conda.sh"],
	local = True,
	size = "enormous",
)

py_library(
	name = "get_sys_exec",
	srcs = ["get-sys-exec.py"],
)