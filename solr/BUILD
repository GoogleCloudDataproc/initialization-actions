package(default_visibility = ["//visibility:public"])

exports_files(["solr.sh"])

py_test(
	name = "test_solr",
	srcs = ["test_solr.py"],
	deps = ["//integration_tests:dataproc_test_case",
			":verify_solr",
			"@io_abseil_py//absl/flags"
		   ],
	data = ["solr.sh"],
	local = True,
	size = "enormous",
)

py_library(
	name = "verify_solr",
	srcs = ["verify_solr.py"],
)