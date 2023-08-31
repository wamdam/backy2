# -*- encoding: utf-8 -*-
from setuptools import setup, find_packages

version = '2.13.8'

setup(name='backy2',
    version=version,
    description="A block / disk based backup and restore solution",
    long_description=open('README.rst', 'r', encoding='utf-8').read(),
    classifiers="""Development Status :: 4 - Beta
Environment :: Console
Intended Audience :: System Administrators
License :: OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)
Operating System :: POSIX
Programming Language :: Python
Programming Language :: Python :: 3
Programming Language :: Python :: 3.6
Programming Language :: Python :: 3.7
Programming Language :: Python :: 3.8
Programming Language :: Python :: 3.9
Topic :: System :: Archiving :: Backup
"""[:-1].split('\n'),
    keywords='backup',
    author='Daniel Kraft <daniel.kraft@d9t.de>',
    author_email='daniel.kraft@d9t.de',
    url='http://backy2.com/',
    license='LGPL-3',
    packages=find_packages('src', exclude=['ez_setup', 'examples', 'tests']),
    package_dir={
        '': 'src',
    },
    package_data={
        'src': ['meta_backends/sql_migrations/alembic.ini'],
    },
    include_package_data=True,
    zip_safe=False,  # ONLY because of alembic.ini. The rest is zip-safe.
    install_requires=[
        'PrettyTable>=0.7.2',
        'sqlalchemy>=1.0.11',  # 1.0.11 is on ubuntu 16.04
        'psutil>=2.2.1',
        'shortuuid>=0.4.2',
        'setproctitle>=1.1.8',
        'python-dateutil>=2.6.0',
        'alembic>=0.7.5',
        'fusepy>=3.0.0',  # TODO: This is not available
        #'pycryptodome>=3.6.1,<4',
        #'zstandard>=0.9.0',
        #'boto>=2.38.0',
        #'psycopg2>=2.6.1',
        #'pex==1.1.0',
        ],
    # tests_require=[
        # 'pytest-cov',
        # 'pytest-capturelog',
        # 'pytest-timeout',
        # 'pytest',
        # ],
    entry_points="""
        [console_scripts]
            backy2 = backy2.scripts.backy:main
    """,
    )
