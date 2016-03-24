# -*- encoding: utf-8 -*-
from setuptools import setup, find_packages

version = '2.4.3'

setup(name='backy2',
    version=version,
    description="A block / disk based backup and restore solution",
    long_description="""\
            """,
    classifiers="""
Development Status :: 4 - Beta
Environment :: Console
Intended Audience :: System Administrators
License :: OSI Approved :: GNU General Public License v3 (GPLv3)
Operating System :: POSIX
Programming Language :: Python
Programming Language :: Python :: 3
Programming Language :: Python :: 3.2
Programming Language :: Python :: 3.3
Programming Language :: Python :: 3.4
Topic :: System :: Archiving :: Backup
"""[:-1].split('\n'),
    keywords='backup',
    author=('Daniel Kraft <daniel.kraft@d9t.de>'
            'Christian Theune <ct@flyingcircus.io>'),
    author_email='daniel.kraft@d9t.de',
    url='https://d9t.de/',
    license='GPL-3',
    packages=find_packages('src', exclude=['ez_setup', 'examples', 'tests']),
    package_dir={
        '': 'src',
    },
    include_package_data=True,
    zip_safe=True,
    install_requires=[
        'pytest-cov',
        'pytest-capturelog',
        'pytest-timeout',
        'PrettyTable==0.7.2',
        'sqlalchemy==1.0.9',
        'psycopg2==2.6.1',
        'boto',
        'pex==1.1.0',
        'psutil',
        'pytest',
        ],
    entry_points="""
        [console_scripts]
            backy = backy2.scripts.backy:main
    """,
    )
