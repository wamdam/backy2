.. include:: global.rst.inc
.. _quickstart:

Quick Start
===========

This guide will show you how to do a *backup* - *scrub* - *restore* - *cleanup*
cycle with **sqlite** as metadata backend and a **file storage** backup target
(e.g. NFS).


What you need to know:
----------------------

Backup source
    A block device or image file to be backed up. Benji can not backup folders
    or multiple files. The source must not be modified during backup, so either
    stop all writers or create a snapshot.

Backup target
    A storage (currently supported: filesystem, S3 and B2) to which the
    backed up data will be saved.

Backup Metadata
    A SQL database containing information on how to reassemble the stored blocks
    to get the original data back.

Version
    A version is a backup of specific backup source at a specific point in time.
    A version is identified by its version UID.


.. _installation:

Installation
------------

Currently there are no pre-built packages but you can easily install Benji
via ``pip``.

Ubuntu 16.04
~~~~~~~~~~~~

This version of Ubuntu doesn't have a current Python installation. But Python 3
via private repository::

    apt-get update
    apt-get install --no-install-recommends software-properties-common python-software-properties
    add-apt-repository ppa:deadsnakes/ppa
    apt-get update
    apt-get --no-install-recommends python3.6 python3.6-venv python3.6-dev git gcc

CentOS 7
~~~~~~~~

As with Ubuntu you need to install a recent Python version from a third-party repository::

    yum install -y https://centos7.iuscommunity.org/ius-release.rpm
    yum install -y python36u-devel python36u-pip python36u-libs python36u-setuptools

Common to all distributions
~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
additional dependencies:

- ``boto3``: AWS S3 backup storage target support
- ``b2``: Backblaze's B2 Cloud storage support
- ``pycryptodome``: Encryption support
- ``discache``: Disk caching support
- ``zstandard``: Compression support
- ``psycopg2-binary`` or ``psycopg2``: PostgreSQL support


Customise your configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This represents a minimal configuration mit SQLite3 backend and file-based block storage::

            configurationVersion: '1.0.0'
            processName: benji
            logFile: /var/log/benji.log
            hashFunction: blake2b,digest_size=32
            blockSize: 4294967296
            io:
              file:
                simultaneousReads: 2
            dataBackend:
              type: file
              file:
                path: /var/lib/benji
            metaBackend:
              engine: sqlite:///var/lib/benji/benji.sqlite

You might need to change the above paths. Benji will run as a normal user, but it
might need root privileges to access some backup sources.

Please see ``etc/benji.yaml`` which is included in the distribution for a full list
of possible configuration options.

.. _backup:

backup
------

1. Initialize the database::

        $ benji initdb
            INFO: $ /usr/bin/benji initdb
            INFO: Benji complete.

   .. NOTE:: Initializing a database multiple times does **not** destroy any
       data, instead will fail because it finds already-existing tables.

2. Create demo data:

   For demonstration purpose, create a 40MB test file::

        $ dd if=/dev/urandom of=TESTFILE bs=1M count=40
        40+0 records in
        40+0 records out
        41943040 bytes (42 MB, 40 MiB) copied, 0.175196 s, 239 MB/s


3. Backup the image (works similar with a device)::

        $ benji backup file://TESTFILE myfirsttestbackup
            INFO: $ /usr/bin/benji backup file://TESTFILE myfirsttestbackup
            INFO: Backed up 1/10 blocks (10.0%)
            INFO: Backed up 2/10 blocks (20.0%)
            INFO: Backed up 3/10 blocks (30.0%)
            INFO: Backed up 4/10 blocks (40.0%)
            INFO: Backed up 5/10 blocks (50.0%)
            INFO: Backed up 6/10 blocks (60.0%)
            INFO: Backed up 7/10 blocks (70.0%)
            INFO: Backed up 8/10 blocks (80.0%)
            INFO: Backed up 9/10 blocks (90.0%)
            INFO: Backed up 10/10 blocks (100.0%)
            INFO: New version: 8fd42f1a-2364-11e7-8594-00163e8c0370 (Tags: [b_daily,b_weekly,b_monthly])
            INFO: Benji complete.


4. List backups::

        $ benji ls
            INFO: $ /usr/bin/benji ls
        +---------------------+-------------------+---------------+------+------------+--------------------------------------+-------+-----------+----------------------------+
        |         date        | name              | snapshot_name | size | size_bytes |                 uid                  | valid | protected | tags                       |
        +---------------------+-------------------+---------------+------+------------+--------------------------------------+-------+-----------+----------------------------+
        | 2017-04-17 11:54:07 | myfirsttestbackup |               |   10 |   41943040 | 8fd42f1a-2364-11e7-8594-00163e8c0370 |   1   |     0     | b_daily,b_monthly,b_weekly |
        +---------------------+-------------------+---------------+------+------------+--------------------------------------+-------+-----------+----------------------------+
            INFO: Benji complete.


scrub
-----

Scrubbing reads all the blocks from the backup target (or some of them if you
use the ``-p`` option) and compares them with the metadata or, if you pass a
source option (``-s``), also with the original data. ::

    $ benji scrub 8fd42f1a-2364-11e7-8594-00163e8c0370
        INFO: $ /usr/bin/benji scrub 8fd42f1a-2364-11e7-8594-00163e8c0370
        INFO: Benji complete.

If an error occurs (for example, if backup target blocks couldn't be read or data
has changed), this looks like that::

    $ benji scrub 8fd42f1a-2364-11e7-8594-00163e8c0370
         INFO: $ /usr/bin/benji scrub 8fd42f1a-2364-11e7-8594-00163e8c0370
        ERROR: Blob not found: Block(uid='2c0910bef8qnAm54mnyBsonRsPBfTzP', version_uid='8fd42f1a-2364-11e7-8594-00163e8c0370', id=8, date=datetime.datetime(2017, 4, 17, 11, 54, 7, 639022), checksum='41c9aa8df42913b3544270a10f1b219cd1b5e1ad9d51700e97acdaeaed3cea843ffaad99590e07de260918ce3847a8b612c9f51f2c945a2d14238956a49ca572', size=4194304, valid=1)
         INFO: Marked block invalid (UID 2c0910bef8qnAm54mnyBsonRsPBfTzP, Checksum 41c9aa8df42913b3544270a10f1b219cd1b5e1ad9d51700e97acdaeaed3cea843ffaad99590e07de260918ce3847a8b612c9f51f2c945a2d14238956a49ca572. Affected versions: 8fd42f1a-2364-11e7-8594-00163e8c0370
         INFO: Marked version invalid (UID 8fd42f1a-2364-11e7-8594-00163e8c0370)
        ERROR: Marked version invalid because it has errors: 8fd42f1a-2364-11e7-8594-00163e8c0370

The exit code is then != 0. Which one exactly depends on the kind of error::

    $ echo $?
    20

Also, the version is marked invalid as you can see in::

    $ benji ls
        INFO: $ /usr/bin/benji ls
    +---------------------+-------------------+---------------+------+------------+--------------------------------------+-------+-----------+----------------------------+
    |         date        | name              | snapshot_name | size | size_bytes |                 uid                  | valid | protected | tags                       |
    +---------------------+-------------------+---------------+------+------------+--------------------------------------+-------+-----------+----------------------------+
    | 2017-04-17 11:54:07 | myfirsttestbackup |               |   10 |   41943040 | 8fd42f1a-2364-11e7-8594-00163e8c0370 |   0   |     0     | b_daily,b_monthly,b_weekly |
    +---------------------+-------------------+---------------+------+------------+--------------------------------------+-------+-----------+----------------------------+
        INFO: Benji complete

Just in case you are able to fix the error, just scrub again and benji will mark the version valid again.


restore
-------

Restore into a file or device::

    $ benji restore 8fd42f1a-2364-11e7-8594-00163e8c0370 file://RESTOREFILE
        INFO: $ /usr/bin/benji restore 8fd42f1a-2364-11e7-8594-00163e8c0370 file://RESTOREFILE
        INFO: Restored 1/10 blocks (10.0%)
        INFO: Restored 2/10 blocks (20.0%)
        INFO: Restored 3/10 blocks (30.0%)
        INFO: Restored 4/10 blocks (40.0%)
        INFO: Restored 5/10 blocks (50.0%)
        INFO: Restored 6/10 blocks (60.0%)
        INFO: Restored 7/10 blocks (70.0%)
        INFO: Restored 8/10 blocks (80.0%)
        INFO: Restored 9/10 blocks (90.0%)
        INFO: Restored 10/10 blocks (100.0%)
        INFO: Benji complete.

benji prevents you from restoring into an existing file (or device). So if you
try again, it will fail::

    $ benji restore 8fd42f1a-2364-11e7-8594-00163e8c0370 file://RESTOREFILE
        INFO: $ /usr/bin/benji restore 8fd42f1a-2364-11e7-8594-00163e8c0370 file://RESTOREFILE
       ERROR: Target already exists: file://RESTOREFILE
    Error opening restore target. You must force the restore.

If you want to overwrite data, you must ``--force``.

.. NOTE:: For more (and possibly faster) restore methods, please refer to the
    section :ref:`restore`.


version rm and cleanup
----------------------

You can remove any given backup version by::

    $ benji rm 8fd42f1a-2364-11e7-8594-00163e8c0370
        INFO: $ /usr/bin/benji rm 8fd42f1a-2364-11e7-8594-00163e8c0370
       ERROR: Unexpected exception
       ERROR: 'Version 8fd42f1a-2364-11e7-8594-00163e8c0370 is too young. Will not delete.'
       […]
        INFO: Benji failed.

What prevents this version to be deleted is the ``backy.cfg`` option ::

    disallow_rm_when_younger_than_days: 6

However, instead of changing this option, you can simply use the ``--force``::

    $ benji rm 8fd42f1a-2364-11e7-8594-00163e8c0370 --force
        INFO: $ /usr/bin/benji rm 8fd42f1a-2364-11e7-8594-00163e8c0370 --force
        INFO: Removed backup version 8fd42f1a-2364-11e7-8594-00163e8c0370 with 10 blocks.
        INFO: Benji complete.

Benji stores each block in the backup target (i.e. filesystem, s3, ...) once.
If it encounters another block on the backup source with the same checksum [1]_,
it will only write metadata which refers to the same backup target block.

So if a backup is deleted, Benji needs to check if all references to each block
are gone. So Benji can't just simply wipe all blocks from a removed backup
version.

As this is a resource-intensive task, it's separated into a special command::

    $ benji cleanup
        INFO: $ /usr/bin/benji cleanup
        INFO: Cleanup-fast: Cleanup finished. 0 false positives, 0 data deletions.
        INFO: Benji complete.

As you can see, nothing has been deleted. The reason for this is that Benji
is prepared to be used in parallel (backups during restores during scrubs).
There are edge cases where blocks might just be in the process of being
referenced, so the cleanup command only considers blocks for deletion when
they have been on the candidate list for a certain time.

For cleanup this time is 1 hour. So you can now wait for 1 hour and repeat,
or you can use the alternative cleanup option (``-f``) which will ignore this
timeout. However be warned (also shown when doing a ``benji cleanup --help``):

    .. NOTE:: A full cleanup must not be run parallel to ANY other Benji jobs.
        Benji will prevent you from doing this by creating a global lock.

.. CAUTION:: Parallelism has been tested successfully with PostgreSQL. It might
    not work reliably with other DBMS.


.. [1] Benji uses blake2b with a 32 byte digest size but this can be configured
    in ``benji.yaml``. blake2b is the recommended hash function as it is very
    fast on modern computers. However it's possible to use any other algorithm
    from Python's hashlib (i.e. ``md5``, ``sha1``, ``sha224``, ``sha256``,
    ``sha384`` or ``sha512``). The maximum supported digest length is 64.
    Smaller digest lengths have a higher chance of hash collisions which must
    be avoided. Digest lengths below 32 bytes are not recommened.
