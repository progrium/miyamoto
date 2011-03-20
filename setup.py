#!/usr/bin/env python

from setuptools import setup

setup(
    name='miyamoto',
    version='0.1',
    author='Jeff Lindsay',
    author_email='jeff.lindsay@twilio.com',
    description='task queue',
    packages=['miyamoto'],
    scripts=[],
    install_requires=['gevent', 'httplib2'],
    data_files=[
        #('/etc/init.d', ['init.d/realtime']),
    ]
)
