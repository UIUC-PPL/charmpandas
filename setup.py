import sys
import os
import subprocess
from setuptools import setup, find_packages


def get_version():
    data = {}
    fname = os.path.join('charmstencil', '__init__.py')
    exec(compile(open(fname).read(), fname, 'exec'), data)
    return data.get('__version__')


def compile_server():
    charmc = os.environ.get('CHARMC', '~/charm/netlrts-linux-x86_64/bin/charmc')
    aum_base = os.environ.get('AUM_HOME', '~/LibAum')
    subprocess.run(["make", "-C", "src/",
                    "CHARMC=%s" % charmc, "BASE_DIR=%s" % aum_base])


install_requires = ['numpy']
tests_require = ['pytest']
docs_require = ['sphinx']

classes = '''
Development Status :: 4 - Beta
Intended Audience :: Developers
Intended Audience :: Science/Research
License :: OSI Approved :: BSD License
Natural Language :: English
Operating System :: MacOS :: MacOS X
Operating System :: POSIX
Operating System :: Unix
Programming Language :: Python
Programming Language :: Python :: 3
Topic :: Software Development :: Libraries
Topic :: Utilities
'''
classifiers = [x.strip() for x in classes.splitlines() if x]

#compile_server()

setup(
    name='charmstencil',
    version=get_version(),
    author='Aditya Bhosale',
    author_email='adityapb1546@gmail.com',
    description='A python library for distributed stencil computations',
    long_description=open('README.rst').read(),
    license="BSD",
    #url='https://github.com/UIUC-PPL/PyProject',
    classifiers=classifiers,
    packages=find_packages(),
    install_requires=install_requires,
    extras_require={
        "docs": docs_require,
        "tests": tests_require,
        "dev": docs_require + tests_require,
    },
)
