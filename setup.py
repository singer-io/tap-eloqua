#!/usr/bin/env python

from setuptools import setup

setup(name='tap-eloqua',
      version='0.6.0',
      description='Singer.io tap for extracting data from the Oracle Eloqua API',
      author='Stitch',
      url='https://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_eloqua'],
      install_requires=[
          'backoff==1.3.2',
          'requests==2.20.1',
          'pendulum==2.0.3',
          'singer-python==5.2.0'
      ],
      entry_points='''
          [console_scripts]
          tap-eloqua=tap_eloqua:main
      ''',
      packages=['tap_eloqua'],
      package_data = {
          'tap_eloqua': ['schemas/*.json'],
      },
)
