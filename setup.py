#!/usr/bin/env python

from setuptools import setup
from distutils.core import setup

setup(name='examplejob',
      version='0.1',
      author='Rex The Analyst',
      author_email='nobody@mozilla.com',
      py_modules=['examplejob'],
      dependency_links=['https://github.com/harterrt/betl/tarball/transfer_code#egg=package-1.0'],
     )


