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
        dependency_links = ['git+ssh://git@bitbucket.org/eileenkuehn/evenmoreutils.git@master#egg=evenmoreutils-bb-0.1', 
                            'git+ssh://git@bitbucket.org/eileenkuehn/dbutils.git@master#egg=dbutils-bb-0.1',
                            'git+ssh://git@bitbucket.org/eileenkuehn/utility.git@master#egg=utility-bb-0.1'],
        install_requires=['psycopg2', 'evenmoreutils', 'dbutils', 'utility'],
)

