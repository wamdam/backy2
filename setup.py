# -*- encoding: utf-8 -*-
version = '0.1.0'

try:
    from setuptools import setup, Extension, find_packages
except ImportError:
    from distutils.core import setup, Extentsion, find_packages

with open("README.rst", "r") as fh:
    long_description = fh.read()

setup(
    name='benji',
    version=version,
    description='A block based deduplicating backup software for Ceph RBD, image files and devices ',
    long_description=long_description,
    long_description_content_type='text/x-rst',
    classifiers="""Development Status :: 3 - Alpha
Environment :: Console
Intended Audience :: System Administrators
License :: OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)
Operating System :: POSIX
Programming Language :: Python :: 3
Programming Language :: Python :: 3.6
Topic :: System :: Archiving :: Backup
""" [:-1].split('\n'),
    keywords='backup',
    author='Daniel Kraft <daniel.kraft@d9t.de>, Lars Fenneberg <lf@elemental.net>',
    author_email='daniel.kraft@d9t.de, lf@elemental.net',
    url='https://github.com/elemental-lf/benji',
    license='LGPL-3',
    packages=find_packages('src', exclude=['*.tests', '*.tests.*']),
    package_dir={
        '': 'src',
    },
    package_data={
        'src': ['sql_migrations/alembic.ini'],
    },
    include_package_data=True,
    zip_safe=False,  # ONLY because of alembic.ini. The rest is zip-safe.
    install_requires=[
        'PrettyTable>=0.7.2',
        'sqlalchemy>=1.2.6',
        'setproctitle>=1.1.8',
        'python-dateutil>=2.6.0',
        'alembic>=0.9.9',
        'ruamel.yaml>=0.15.37',
        'psycopg2-binary>=2.7.4',
        'argcomplete>=1.9.4',
        'sparsebitfield>=0.2.2',
        'colorlog>=3.1.4',
    ],
    extras_require={
        's3_boto3 data backend': ['boto3>=1.7.28'],
        'encryption': ['pycryptodome>=3.6.1', 'aes-keywrap>17.12.1'],
        'compression': ['zstandard>=0.9.0'],
        'disk based read cache': ['diskcache>=3.0.6'],
        # For RBD support the packages supplied by the Linux distribution or the Ceph team should be used,
        # possible packages names include: python-rados, python-rbd or python3-rados, python3-rbd
        #'RBD support': ['rados', 'rbd'],
    },
    python_requires='~=3.6',
    entry_points="""
        [console_scripts]
            benji = benji.scripts.benji:main
    """,
)
