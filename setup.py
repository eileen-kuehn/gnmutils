#!/usr/bin/env python

import setuptools
import platform
from distutils.core import setup

INSTALL_REQUIRES = ['evenmoreutils==0.1', 'utility==0.1']
if 'pypy' not in platform.python_implementation().lower():
    INSTALL_REQUIRES.append('psycopg2')
    INSTALL_REQUIRES.append('dbutils==0.1')'

setup(
        name="gnmutils",
        version="0.1",
        description="Utilities for GridKa Network Monitor",
        author="Eileen Kuehn",
        author_email="eileen.kuehn@kit.edu",
        url="https://bitbucket.org/eileenkuehn/gnmutils",
        packages=setuptools.find_packages(),
        dependency_links = ['git+ssh://git@bitbucket.org/eileenkuehn/evenmoreutils.git@master#egg=evenmoreutils-0.1',
                            'git+ssh://git@bitbucket.org/eileenkuehn/dbutils.git@master#egg=dbutils-0.1',
                            'git+ssh://git@bitbucket.org/eileenkuehn/utility.git@master#egg=utility-0.1'],
        install_requires=INSTALL_REQUIRES,
)

