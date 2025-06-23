#!/usr/bin/env python

from setuptools import setup

setup(name='tap-eloqua',
      version='1.3.1',
      description='Singer.io tap for extracting data from the Oracle Eloqua API',
      author='Stitch',
      url='https://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_eloqua'],
      install_requires=[
          'backoff==1.10.0',
          'requests==2.32.4',
          'pendulum==2.0.3',
          'singer-python==5.13.2'
      ],
      extras_require={
          'dev': [
              'ipdb==0.11'
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
