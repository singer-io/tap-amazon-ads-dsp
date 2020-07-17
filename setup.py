#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='tap-amazon-ads-dsp',
      version='0.0.1',
      description='Singer.io tap for extracting data from the Amazon Advertising DSP v1.0 API',
      author='scott.coleman@bytecode.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_amazon_ads_dsp'],
      install_requires=[
          'backoff==1.8.0',
          'requests==2.23.0',
          'singer-python==5.8.1',
          'pycurl==7.43.0.5',
          'requests_oauthlib==1.3.0',
      ],
      entry_points='''
          [console_scripts]
          tap-amazon-ads-dsp=tap_amazon_ads_dsp:main
      ''',
      packages=find_packages(),
      package_data={
          'tap_amazon_ads_dsp': [
              'schemas/*.json',
              'tests/*.py'
          ]
      })
