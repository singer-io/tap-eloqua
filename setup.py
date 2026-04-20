#!/usr/bin/env python

from setuptools import setup

setup(name='tap-eloqua',
      version='1.4.0',
      description='Singer.io tap for extracting data from the Oracle Eloqua API',
      author='Stitch',
      url='https://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_eloqua'],
      install_requires=[
          'backoff==2.2.1',
          'requests==2.33.1',
          'pendulum==3.2.0',
          'singer-python==6.8.0'
      ],
      extras_require={
          'dev': [
              'ipdb',
              'pylint',
          ]
      },
      entry_points='''
          [console_scripts]
          tap-eloqua=tap_eloqua:main
      ''',
      packages=['tap_eloqua'],
      package_data = {
          'tap_eloqua': ['schemas/*.json'],
      },
)
