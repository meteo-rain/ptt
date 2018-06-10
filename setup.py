#!/usr/bin/env python3

from distutils.core import setup

setup(name='ptt',
      version='1.0',
      description='Meteorain PTT Crawler & Data Manipulation Library',
      author='Roger Kuo',
      author_email='rogerkuo1689@gmail.com',
      url='https://github.com/meteo-rain/ptt',
      package_dir= {'ptt': 'src'},
      packages=['ptt'],
     )