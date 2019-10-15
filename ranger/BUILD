package(default_visibility = ["//visibility:public"])

py_test(
	name = "test_ranger",
	srcs = ["test_ranger.py"],
	deps = ["//integration_tests:dataproc_test_case",
			":verify_ranger",
			"@io_abseil_py//absl/flags"],
	data = ["ranger.sh",
			"//solr:solr.sh"],
	local = True,
	size = "enormous",
)

py_library(
	name = "verify_ranger",
	srcs = ["verify_ranger.py"],
)