load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

http_archive(
    name = "rules_python",
    sha256 = "aa96a691d3a8177f3215b14b0edc9641787abaaa30363a080165d06ab65e1161",
    url = "https://github.com/bazelbuild/rules_python/releases/download/0.0.1/rules_python-0.0.1.tar.gz",
)

http_archive(
    name = "io_abseil_py",
    sha256 = "0a145cb81101d1add8b87eaae58c5d51521084bf7cc4e4654928b326a864c6c3",
    strip_prefix = "abseil-py-pypi-v0.8.1",
    url = "https://github.com/abseil/abseil-py/archive/pypi-v0.8.1.tar.gz",
)

http_archive(
    name = "six_archive",
    build_file = "@io_abseil_py//third_party:six.BUILD",
    sha256 = "105f8d68616f8248e24bf0e9372ef04d3cc10104f1980f54d57b2ce73a5ad56a",
    strip_prefix = "six-1.10.0",
    urls = [
        "http://mirror.bazel.build/pypi.python.org/packages/source/s/six/six-1.10.0.tar.gz",
        "https://pypi.python.org/packages/source/s/six/six-1.10.0.tar.gz",
    ],
)
