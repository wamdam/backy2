.. include:: global.rst.inc
.. _quickstart:

Quick Start
===========

This guide will show you how to do a *backup* - *scrub* - *restore* - *cleanup*
cycle with **sqlite** as metadata backend and a **file storage** backup target
(e.g. NFS).


Some Vocabulary
---------------

Backup source
    A block device or image file to be backed up. Benji can't backup folders
    or multiple files. The source must not be modified during backup, so either
    stop all writes or create a snapshot.

Backup target
    A data storage (currently supported: filesystem, S3 and B2) to which the
    backed up data will be saved. Also referred to as the *data backend*.

Backup Metadata
    A SQL database containing information on how to reassemble the stored blocks
    to get the original data back. Also referred to as the *metadata backend*.

Version
    A version is a backup of a specific backup source at a specific point in time.
    A version is identified by a unique id.

.. _backup:

Backup
------

1. Initialize the database::

    $ benji initdb
        INFO: $ benji initdb

   .. NOTE:: Initializing a database multiple times does **not** destroy any
       data, instead it will fail because it finds already existing tables.

2. Create demo data:

   For demonstration purpose, create a 40MB test file::

    $ dd if=/dev/urandom of=TESTFILE bs=1M count=40
    40+0 records in
    40+0 records out
    41943040 bytes (42 MB, 40 MiB) copied, 0.231886 s, 181 MB/s

3. Backup the image (works similar with a device)::

    $ benji backup file://TESTFILE myfirsttestbackup
        INFO: $ benji backup file://TESTFILE myfirsttestbackup
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
        INFO: Exported version V0000000001 metadata to backend storage.
        INFO: New version: V0000000001 (Tags: [])

4. List backups::

    $ benji ls
        INFO: $ benji ls
    +---------------------+-------------+-------------------+---------------+----------+------------+-------+-----------+------+
    |         date        |     uid     | name              | snapshot_name |     size | block_size | valid | protected | tags |
    +---------------------+-------------+-------------------+---------------+----------+------------+-------+-----------+------+
    | 2018-06-06T21:41:41 | V0000000001 | myfirsttestbackup |               | 41943040 |    4194304 |  True |   False   |      |
    +---------------------+-------------+-------------------+---------------+----------+------------+-------+-----------+------+

Some commands (like ``ls``, ``stats``, ``backup`` and ``enforce``) can also produce
machine readable JSON output for usage in scripts::

    $ benji -m ls
    {
      "metadataVersion": "1.0.0",
      "versions": [
        {
          "uid": 1,
          "date": "2018-06-06T21:41:41",
          "name": "myfirsttestbackup",
          "snapshot_name": "",
          "size": 41943040,
          "block_size": 4194304,
          "valid": true,
          "protected": false,
          "tags": []
        }
      ]
    }

Specifying ``-m`` automatically turns down the verbosity level to only output
errors.

Deep-scrub and Scrub
--------------------

Deep scrubbing reads all the blocks from the backup target (or some of them if you
use the ``-p`` option) and compares them with the metadata or, if you pass a
source option (``-s``), also with the original data. ::

    $ benji deep-scrub v1
        INFO: $ benji deep-scrub v1
        INFO: Deep scrubbed 1/10 blocks (10.0%)
        INFO: Deep scrubbed 2/10 blocks (20.0%)
        INFO: Deep scrubbed 3/10 blocks (30.0%)
        INFO: Deep scrubbed 4/10 blocks (40.0%)
        INFO: Deep scrubbed 5/10 blocks (50.0%)
        INFO: Deep scrubbed 6/10 blocks (60.0%)
        INFO: Deep scrubbed 7/10 blocks (70.0%)
        INFO: Deep scrubbed 8/10 blocks (80.0%)
        INFO: Deep scrubbed 9/10 blocks (90.0%)
        INFO: Deep scrubbed 10/10 blocks (100.0%)

If an error occurs (for example, if backup target blocks couldn't be read or data
has changed), the output from ``deep-scrub`` looks like this::

    $ benji deep-scrub v1
        INFO: $ benji deep-scrub v1
        INFO: Deep scrubbed 1/10 blocks (10.0%)
        INFO: Deep scrubbed 2/10 blocks (20.0%)
        INFO: Deep scrubbed 3/10 blocks (30.0%)
        INFO: Deep scrubbed 4/10 blocks (40.0%)
        INFO: Deep scrubbed 5/10 blocks (50.0%)
        INFO: Deep scrubbed 6/10 blocks (60.0%)
       ERROR: Checksum mismatch during deep scrub for block 6 (UID 1-7) (is: 729a77dc964e5f543e6f10697386429d5978a1681a86fce1101aa2abcb41c5cc should-be: b70aeb070b95df31f68fd19c99e33f2826bd2c50049ca48c27b58743ab8a8d64).
        INFO: Marked block invalid (UID 1-7, Checksum b70aeb070b95df31. Affected versions: V0000000001
        INFO: Marked version invalid (UID V0000000001)
        INFO: Deep scrubbed 8/10 blocks (80.0%)
        INFO: Deep scrubbed 9/10 blocks (90.0%)
        INFO: Deep scrubbed 10/10 blocks (100.0%)
       ERROR: Marked version V0000000001 invalid because it has errors.
       ERROR: Deep scrub of version V0000000001 failed.

In case of a scrubbing error the exit code is non-zero. A failed scrub is
signaled by EX_IOERR which is 74 on Linux.

Also, the version is marked invalid as you can see here::

    $ benji ls
        INFO: $ benji ls
    +---------------------+-------------+-------------------+---------------+----------+------------+-------+-----------+------+
    |         date        |     uid     | name              | snapshot_name |     size | block_size | valid | protected | tags |
    +---------------------+-------------+-------------------+---------------+----------+------------+-------+-----------+------+
    | 2018-06-06T21:41:41 | V0000000001 | myfirsttestbackup |               | 41943040 |    4194304 | False |   False   |      |
    +---------------------+-------------+-------------------+---------------+----------+------------+-------+-----------+------+

Just in case you are able to fix the error, just scrub again and Benji will mark the version valid again.

There is also a little brother to ``deep-scrub`` which only check for metadata consistency and block existence::

    $ benji scrub v1
        INFO: $ benji scrub v1
        INFO: Scrubbed 1/10 blocks (10.0%)
        INFO: Scrubbed 2/10 blocks (20.0%)
        INFO: Scrubbed 3/10 blocks (30.0%)
        INFO: Scrubbed 4/10 blocks (40.0%)
        INFO: Scrubbed 5/10 blocks (50.0%)
        INFO: Scrubbed 6/10 blocks (60.0%)
        INFO: Scrubbed 7/10 blocks (70.0%)
        INFO: Scrubbed 8/10 blocks (80.0%)
        INFO: Scrubbed 9/10 blocks (90.0%)
        INFO: Scrubbed 10/10 blocks (100.0%)

``scrub`` will only mark versions as invalid never as valid. This is because there
isn't enough information to determine if the version is really okay when only
checking metadata consistency and block existence. A ``scrub`` of an invalid version
will fail immediately.

Restore
-------

Restore into a file or device::

    $ benji restore v1 file://RESTOREFILE
        INFO: $ benji restore v1 file://RESTOREFILE
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

Benji prevents you from restoring into an existing file (or device). So if you
try again, it will fail::

    $ benji restore v1 file://RESTOREFILE
        INFO: $ benji restore v1 file://RESTOREFILE
       ERROR: Restore target RESTOREFILE already exists. Force the restore if you want to overwrite it.

If you want to overwrite data, you must ``--force``.

.. NOTE:: For more (and possibly faster) restore methods, please refer to the
    section :ref:`restore`.


Version Removal and Cleanup
---------------------------

You can remove any given backup version by::

    $ benji rm 8fd42f1a-2364-11e7-8594-00163e8c0370
        INFO: $ /usr/bin/benji rm 8fd42f1a-2364-11e7-8594-00163e8c0370
       ERROR: Unexpected exception
       ERROR: 'Version 8fd42f1a-2364-11e7-8594-00163e8c0370 is too young. Will not delete.'
       […]
        INFO: Benji failed.

What prevents this version from being deleted is the ``benji.yaml`` option::

    disallowRemoveWhenYounger: 6

However, instead of changing this option, you can simply use ``--force``::

    $ benji rm -f v1
        INFO: $ benji rm -f v1
        INFO: Removed version V0000000001 metadata from backend storage.
        INFO: Removed backup version V0000000001 with 10 blocks.

Benji stores each block in the backup target (i.e. filesystem, S3, etc.) once.
If it encounters another block on the backup source with the same checksum [1]_,
it will only write metadata which refers to the same backup target block. So if
a version is deleted, Benji needs to check if there aren't any other references
to any of the blocks referenced by this version. This may be resource intensive
but may also introduce race conditions due to other backup sessions running
in parallel. This is why there is a separate command to cleanup unreferenced
blocks::

    $ benji cleanup
        INFO: $ benji cleanup
        INFO: Cleanup-fast: Cleanup finished. 0 false positives, 0 data deletions.

As you can see, nothing has been deleted. The reason for this is that only
blocks  which have been on the candidate list for a certain time (1h) are considered
for deletion to prevent race conditions. If we would have waited on hour after
removing the version, we'd get a slightly different output which indicated that
ten blocks have been permanently deleted::

    $ benji cleanup
        INFO: $ benji cleanup
        INFO: Cleanup-fast: Cleanup finished. 0 false positives, 10 data deletions.

There is also the option (``-f`` or ``--full``) to do a full cleanup which iterates
through all the blocks in the backend storage. This should only be used when an
inconsistency is suspected as it can take a very long time to complete.

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
