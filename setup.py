#!/usr/bin/env python

import setuptools
from distutils.core import setup

setup(
        name="gnmutils",
        version="0.1",
        description="Utilities for GridKa Network Monitor",
        author="Eileen Kuehn",
        author_email="eileen.kuehn@kit.edu",
        url="https://bitbucket.org/eileenkuehn/gnmutils",
        packages=setuptools.find_packages(),
        install_requires=['psycopg2'],
)

