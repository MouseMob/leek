from setuptools import setup
from leek import __version__

setup(
    name='leek',
    version=__version__,
    description='Highly available celery scheduler',
    author='Ben Morris',
    author_email='ben@bendmorris.com',
    py_modules=['leek'],
)
