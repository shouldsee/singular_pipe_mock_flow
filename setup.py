
#!/usr/bin/env python
#from setuptools import setup
from distutils.core import setup
import os,glob,sys
assert sys.version_info >= (3,5),('Requires python>=3.5, found python==%s'%('.'.join([str(x) for x in sys.version_info[:3]])))
config = dict(
	name='singular_pipe_mock_flow',
	version = '0.0.1',
  packages=['.'],
	include_package_data=True,
	license='MIT',
	author='Feng Geng',
	author_email='shouldsee.gem@gmail.com',
	long_description=open('README.md').read(),
	classifiers = [
	'Programming Language :: Python :: 3.5',
	'Programming Language :: Python :: 3.7',
	],
	install_requires=[
		x.strip() for x in open("requirements.txt","r")
        	if x.strip() and not x.strip().startswith("#")
	],

)
setup(**config)

