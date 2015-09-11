# -*- encoding: utf-8 -*-
from setuptools import setup, find_packages

version = '2.0'

setup(name='backy2',
    version=version,
    description="A block / disk based backup and restore solution",
    long_description="""\
            """,
    classifiers=[], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
    keywords='',
    author='Daniel Kraft',
    author_email='daniel.kraft@d9t.de',
    url='https://d9t.de/',
    license='',
    packages=find_packages('src', exclude=['ez_setup', 'examples', 'tests']),
    package_dir={
        '': 'src',
    },
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'PrettyTable==0.7.2',
        ],
    entry_points="""
    """,
    )
