#!/usr/bin/env python

import setuptools
import platform
from distutils.core import setup

INSTALL_REQUIRES = []
if 'pypy' not in platform.python_implementation().lower():
    INSTALL_REQUIRES.append('psycopg2')

setup(
        name="gnmutils",
        version="0.1",
        description="Utilities for GridKa Network Monitor",
        author="Eileen Kuehn",
        author_email="eileen.kuehn@kit.edu",
        url="https://bitbucket.org/eileenkuehn/gnmutils",
        packages=setuptools.find_packages(),
        dependency_links = [],
        install_requires=INSTALL_REQUIRES,
        # unit tests
        test_suite='gnmutils_tests',
)
