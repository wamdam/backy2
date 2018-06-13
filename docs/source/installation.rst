.. include:: global.rst.inc
.. _installation:

Installation
============

Currently there are no pre-built packages but you can easily install Benji
with ``pip``.

Ubuntu 16.04
------------

This version of Ubuntu doesn't have a current Python installation. But Python 3
can be installed via private repository::

    apt-get update
    apt-get install --no-install-recommends software-properties-common python-software-properties
    add-apt-repository ppa:deadsnakes/ppa
    apt-get update
    apt-get --no-install-recommends python3.6 python3.6-venv python3.6-dev git gcc

.. NOTE:: For more information about this Personal Package Archive (PPA)
    please see https://launchpad.net/~deadsnakes/+archive/ubuntu/ppa.

CentOS 7
--------

As with Ubuntu you need to install a recent Python version from a third-party repository::

    yum install -y https://centos7.iuscommunity.org/ius-release.rpm
    yum install -y python36u-devel python36u-pip python36u-libs python36u-setuptools

Redhat Enterprise Linux 7
-------------------------

Almost the some procedure as on CentOS::

    yum install -y https://rhel7.iuscommunity.org/ius-release.rpm
    yum install -y python36u-devel python36u-pip python36u-libs python36u-setuptools

.. NOTE:: For more information about the IOS Community Project please see https://ius.io/.

Common to All Distributions
---------------------------

After installing a recent Python version, it is now time to install
Benji and its dependencies::

    # Create new virtual environment
    python3.6 -m venv /usr/local/beni
    # Activate it (your shell prompt should change)
    . /usr/local/benji/bin/activate
    # Let's upgrade pip first
    pip install --upgrade pip
    # And now install Benji and its dependencies
    pip install git+https://github.com/elemental-lf/benji
    pip install git+https://github.com/kurtbrose/aes_keywrap

.. NOTE:: aes_keywrap is available on PyPI, but the version there isn't
    compatible with Python 3. The source repository contains the
    necessary changes.

If you want to use certain features of Benji in the future you might
want to install additional dependencies:

- ``boto3``: AWS S3 *data backend* support
- ``b2``: Backblaze's B2 Cloud *data backend* support
- ``pycryptodome``: Encryption support
- ``diskcache``: Disk caching support
- ``zstandard``: Compression support

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
without problems, but it will probably need root privileges to access most
backup sources.

The configuration above is suited to jump right into the section
:ref:`quickstart` and try out everything.

Please see :ref:`configuration` for a full list of configuration options.
