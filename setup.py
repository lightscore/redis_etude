#!/usr/bin/env python

import logging
import sys
from distutils.core import setup

if sys.version_info < (3, 5):
    logging.critical('This program uses features from python3.5+ (tested on python3.7). '
                     'Please update your Python interpreter.')
    exit(1)

setup(name='RedisEtude',
      version='1.0',
      description='Redis Distributed Producer / Consumer App',
      author='Mikhail Zhirnov',
      author_email='lightscore@gmail.com',
      url='https://github.com/lightscore/redis_etude',
      packages=['redis_etude'])
