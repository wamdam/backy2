.. include:: global.rst.inc
.. _installation:

Installation
============

Currently there are no pre-built packages but you can easily install Benji
via ``pip``.

Ubuntu 16.04
------------

This version of Ubuntu doesn't have a current Python installation. But Python 3
via private repository::

    apt-get update
    apt-get install --no-install-recommends software-properties-common python-software-properties
    add-apt-repository ppa:deadsnakes/ppa
    apt-get update
    apt-get --no-install-recommends python3.6 python3.6-venv python3.6-dev git gcc

CentOS 7
--------

As with Ubuntu you need to install a recent Python version from a third-party repository::

    yum install -y https://centos7.iuscommunity.org/ius-release.rpm
    yum install -y python36u-devel python36u-pip python36u-libs python36u-setuptools

Common to All Distributions
---------------------------

After installing a recent Python version above, it is now time to install
Benji and its dependencies::

    # Create new virtual environment
    python3.6 -m venv /usr/local/beni
    # Activate it (your shell prompt will change)
    . /usr/local/benji/bin/activate
    # Let's upgrade pip first
    pip install --upgrade pip
    # And now install Benji and its dependencies
    pip install git+https://github.com/elemental-lf/benji
    pip install git+https://github.com/kurtbrose/aes_keywrap

If you want to use certain features of Benji in the future you might
want to install additional dependencies:

- ``boto3``: AWS S3 backup storage target support
- ``b2``: Backblaze's B2 Cloud storage support
- ``pycryptodome``: Encryption support
- ``diskcache``: Disk caching support
- ``zstandard``: Compression support
- ``psycopg2-binary`` or ``psycopg2``: PostgreSQL support

Customise Your Configuration
----------------------------

This represents a minimal configuration mit SQLite3 backend and file-based block storage::

            configurationVersion: '1.0.0'
            processName: benji
            logFile: /var/log/benji.log
            hashFunction: blake2b,digest_size=32
            blockSize: 4194304
            io:
              file:
                simultaneousReads: 2
            dataBackend:
              type: file
              file:
                path: /var/lib/benji
            metadataBackend:
              engine: sqlite:///var/lib/benji/benji.sqlite

You might need to change the above paths. Benji will run as a normal user
without problems, but it might need root privileges to access most backup
sources.

Please see :ref:`configuration` for a full list of configuration options.
