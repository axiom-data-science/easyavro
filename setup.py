from __future__ import with_statement

from setuptools import setup, find_packages


def version():
    with open('VERSION') as f:
        return f.read().strip()


def readme():
    with open('README.md') as f:
        return f.read()


reqs = [line.strip() for line in open('requirements.txt') if not line.startswith('#')]


setup(
    name                = "easyavro",
    version             = version(),
    description         = "A python helper for producing and consuming avro schema'd Kafka topics",
    long_description    = readme(),
    license             = 'MIT',
    author              = "Kyle Wilcox",
    author_email        = "kyle@axiomdatascience.com",
    url                 = "https://github.com/axiom-data-science/easyavro",
    packages            = find_packages(),
    install_requires    = reqs,
    classifiers         = [
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python',
        'Topic :: Scientific/Engineering',
    ],
    include_package_data = True,
)
