
#!/usr/bin/env python
#from setuptools import setup
### dummy touch
from distutils.core import setup
import os,glob,sys
assert sys.version_info >= (3,5),('Requires python>=3.5, found python==%s'%('.'.join([str(x) for x in sys.version_info[:3]])))

config = dict(
	name='spiper_mock_flow',
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
# 		'spiper@https://github.com/shouldsee/python-singular-pipe/tarball/0.0.4', 
		#### spiper is a runtime requirement and is assumed installed
	],
# 	install_requires=[
# 		x.strip() for x in open("requirements.txt","r")
#         	if x.strip() and not x.strip().startswith("#")
# 	],

)
setup(**config)

