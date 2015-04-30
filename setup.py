from codecs import open as codecs_open
from setuptools import setup


# Get the long description from the relevant file
with codecs_open('README.rst', encoding='utf-8') as f:
    long_description = f.read()


setup(name='snapsat-worker',
        version='0.1.0',
        description='Builds composite Landsat images.',
        long_description=long_description,
        keywords='landsat, geospatial',
        url='http://github.com/recombinators/worker',
        author='Snapsat',
        author_email='snapsat@snapsat.org',
        license='MIT',
        packages=['snapsat-worker'],
        zip_safe=False)
