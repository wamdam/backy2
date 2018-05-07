# -*- encoding: utf-8 -*-
from setuptools import setup, find_packages

version = '2.9.17'

setup(name='backy2',
    version=version,
    description="A block / disk based backup and restore solution",
    long_description=open('README.rst', 'r', encoding='utf-8').read(),
    classifiers="""Development Status :: 3 - Alpha
Environment :: Console
Intended Audience :: System Administrators
License :: OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)
Operating System :: POSIX
Programming Language :: Python :: 3
Programming Language :: Python :: 3.6
Topic :: System :: Archiving :: Backup
"""[:-1].split('\n'),
    keywords='backup',
    author='Daniel Kraft <daniel.kraft@d9t.de>, Lars Fenneberg <lf@elemental.net>',
    author_email='daniel.kraft@d9t.de, lf@elemental.net',
    url='https://github.com/elemental-lf/backy2',
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
        'sqlalchemy>=1.0.8',
        'psutil>=2.2.1',
        'shortuuid>=0.4.2',
        'setproctitle>=1.1.8',
        'python-dateutil>=2.6.0',
        'alembic',
        'ruamel.yaml>=0.15.37',
        #'boto>=2.38.0',
        #'boto3>=1.6.16',
        #'psycopg2>=2.7.4',
        #'pex==1.1.0',
        #'aws_s3_cse>=0.2',
        ],
    python_requires='~=3.6',
    entry_points="""
        [console_scripts]
            backy2 = backy2.scripts.backy:main
            cse_derive_key = backy2.scripts.cse_derive_key:main
    """,
    )
