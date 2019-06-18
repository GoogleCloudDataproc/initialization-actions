from setuptools import setup, find_packages

setup(
    name = 'fastunit',
    version = '0.0.1',
    keywords='async unittest corotine',
    description = 'a library for running test cases asynchronously.',
    license = 'MIT License',
    url = 'https://github.com/ityoung/fastunit',
    author = 'Shin Yeung',
    author_email = 'ityoung@foxmail.com',
    packages = find_packages(),
    include_package_data = True,
    platforms = 'any',
    install_requires = [],
)

classifiers = [
    'Development Status :: 3 - Alpha',
    'Intended Audience :: Developers',
    'License :: OSI Approved :: MIT License',
    'Operating System :: POSIX',
    'Operating System :: Microsoft :: Windows',
    'Operating System :: MacOS :: MacOS X',
    'Topic :: Software Development :: Testing',
    'Topic :: Software Development :: Libraries',
    'Topic :: Utilities',
] + [
    ('Programming Language :: Python :: %s' % x)
    for x in '3.5 3.6'.split()
]