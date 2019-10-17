package(default_visibility = ["//visibility:public"])

py_test(
	name = "test_ganglia",
	srcs = ["test_ganglia.py"],
	deps = ["//integration_tests:dataproc_test_case",
			":verify_ganglia_running"],
	local = True,
	size = "enormous",
)

py_library(
	name = "verify_ganglia_running",
	srcs = ["verify_ganglia_running.py"],
)