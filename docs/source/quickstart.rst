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
    A block device or image file (e.g. containing a VM) to be backed up.
    backy2 can not backup folders or multiple files. The source must not be
    modified during backup, so either stop all writers or create a snapshot.

Backup target
    A storage (currently supported: filesystem or S3 compatible) to which the
    backed up data will be saved in 4MB blocks.

Backup Metadata
    A Database containing information on how to reassemble the stored blocks
    to get the original data back. Currently an SQL database.

Version
    A version of a backup. A version is a backup on a specific time for a
    specific backup source. A version is identified by its UUID.


.. _installation:

Installation
------------

Ubuntu 16.04
~~~~~~~~~~~~

Installation::

    wget https://github.com/wamdam/backy2/releases/download/2.9.9/backy2_2.9.11_all.deb
    sudo dpkg -i backy2_2.9.11_all.deb  # Install the debian archive
    sudo apt-get -f install            # Install the dependencies

.. TODO: Show how to install drivers for postgreSQL, MySQL, others

Edit backy.cfg::

    vim /etc/backy.cfg

Especially look if these paths are good.

1. Metadata storage path ::

       engine: sqlite:////var/lib/backy2/backy.sqlite

2. Data storage path ::

       path: /var/lib/backy2/data  # This should be the mountpoint of NFS

Other values of interest are ``simultaneous_writes`` and ``simultaneous_reads``.
Depending on your backup source and target you may want to go lower (i.e.
disk with very slow seeks) or higher (raid source or target, S3 target, ...).

.. TIP::
    For reference, we use about the half the number of disks as value for
    simultaneous access. So if you have 40 OSDs in ceph/rbd on the backup
    source and a 20 disk raid 10 backup target (which makes only 10 parallel
    disks on writes), you'd use ::

        simultaneous_reads: 20
        simultaneous_writes: 5

    Of course your mileage may vary.

These settings have great impact on the backup and restore performance. Higher
values need a bit more RAM.

.. _backup:

backup
------

1. Initialize the database::

        $ backy2 initdb
            INFO: $ /usr/bin/backy2 initdb
            INFO: Backy complete.

   .. NOTE:: Initializing a database multiple times does **not** destroy any
       data, instead will fail because it finds already-existing tables.

2. Create demo data:

   For demonstration purpose, create a 40MB test file::

        $ dd if=/dev/urandom of=TESTFILE bs=1M count=40
        40+0 records in
        40+0 records out
        41943040 bytes (42 MB, 40 MiB) copied, 0.175196 s, 239 MB/s


3. Backup the image (works similar with a device)::

        $ backy2 backup file://TESTFILE myfirsttestbackup
            INFO: $ /usr/bin/backy2 backup file://TESTFILE myfirsttestbackup
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
            INFO: Backy complete.


4. List backups::

        $ backy2 ls
            INFO: $ /usr/bin/backy2 ls
        +---------------------+-------------------+---------------+------+------------+--------------------------------------+-------+-----------+----------------------------+
        |         date        | name              | snapshot_name | size | size_bytes |                 uid                  | valid | protected | tags                       |
        +---------------------+-------------------+---------------+------+------------+--------------------------------------+-------+-----------+----------------------------+
        | 2017-04-17 11:54:07 | myfirsttestbackup |               |   10 |   41943040 | 8fd42f1a-2364-11e7-8594-00163e8c0370 |   1   |     0     | b_daily,b_monthly,b_weekly |
        +---------------------+-------------------+---------------+------+------------+--------------------------------------+-------+-----------+----------------------------+
            INFO: Backy complete.


scrub
-----

Scrubbing reads all the blocks from the backup target (or some of them if you
use the ``-p`` option) and compares them with the metadata or, if you pass a
source option (``-s``), also with the original data. ::

    $ backy2 scrub 8fd42f1a-2364-11e7-8594-00163e8c0370
        INFO: $ /usr/bin/backy2 scrub 8fd42f1a-2364-11e7-8594-00163e8c0370
        INFO: Backy complete.

If an error occurs (for example, if backup target blocks couldn't be read or data
has changed), this looks like that::

    $ backy2 scrub 8fd42f1a-2364-11e7-8594-00163e8c0370
         INFO: $ /usr/bin/backy2 scrub 8fd42f1a-2364-11e7-8594-00163e8c0370
        ERROR: Blob not found: Block(uid='2c0910bef8qnAm54mnyBsonRsPBfTzP', version_uid='8fd42f1a-2364-11e7-8594-00163e8c0370', id=8, date=datetime.datetime(2017, 4, 17, 11, 54, 7, 639022), checksum='41c9aa8df42913b3544270a10f1b219cd1b5e1ad9d51700e97acdaeaed3cea843ffaad99590e07de260918ce3847a8b612c9f51f2c945a2d14238956a49ca572', size=4194304, valid=1)
         INFO: Marked block invalid (UID 2c0910bef8qnAm54mnyBsonRsPBfTzP, Checksum 41c9aa8df42913b3544270a10f1b219cd1b5e1ad9d51700e97acdaeaed3cea843ffaad99590e07de260918ce3847a8b612c9f51f2c945a2d14238956a49ca572. Affected versions: 8fd42f1a-2364-11e7-8594-00163e8c0370
         INFO: Marked version invalid (UID 8fd42f1a-2364-11e7-8594-00163e8c0370)
        ERROR: Marked version invalid because it has errors: 8fd42f1a-2364-11e7-8594-00163e8c0370

The exit code is then != 0. Which one exactly depends on the kind of error::

    $ echo $?
    20

Also, the version is marked invalid as you can see in::

    $ backy2 ls
        INFO: $ /usr/bin/backy2 ls
    +---------------------+-------------------+---------------+------+------------+--------------------------------------+-------+-----------+----------------------------+
    |         date        | name              | snapshot_name | size | size_bytes |                 uid                  | valid | protected | tags                       |
    +---------------------+-------------------+---------------+------+------------+--------------------------------------+-------+-----------+----------------------------+
    | 2017-04-17 11:54:07 | myfirsttestbackup |               |   10 |   41943040 | 8fd42f1a-2364-11e7-8594-00163e8c0370 |   0   |     0     | b_daily,b_monthly,b_weekly |
    +---------------------+-------------------+---------------+------+------------+--------------------------------------+-------+-----------+----------------------------+
        INFO: Backy complete

Just in case you are able to fix the error, just scrub again and backy2 will mark the version valid again.


restore
-------

Restore into a file or device::

    $ backy2 restore 8fd42f1a-2364-11e7-8594-00163e8c0370 file://RESTOREFILE
        INFO: $ /usr/bin/backy2 restore 8fd42f1a-2364-11e7-8594-00163e8c0370 file://RESTOREFILE
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
        INFO: Backy complete.

backy2 prevents you from restoring into an existing file (or device). So if you
try again, it will fail::

    $ backy2 restore 8fd42f1a-2364-11e7-8594-00163e8c0370 file://RESTOREFILE
        INFO: $ /usr/bin/backy2 restore 8fd42f1a-2364-11e7-8594-00163e8c0370 file://RESTOREFILE
       ERROR: Target already exists: file://RESTOREFILE
    Error opening restore target. You must force the restore.

If you want to overwrite data, you must ``--force``.

.. NOTE:: For more (and possibly faster) restore methods, please refer to the
    section :ref:`restore`.


version rm and cleanup
----------------------

You can remove any given backup version by::

    $ backy2 rm 8fd42f1a-2364-11e7-8594-00163e8c0370
        INFO: $ /usr/bin/backy2 rm 8fd42f1a-2364-11e7-8594-00163e8c0370
       ERROR: Unexpected exception
       ERROR: 'Version 8fd42f1a-2364-11e7-8594-00163e8c0370 is too young. Will not delete.'
       […]
        INFO: Backy failed.

What prevents this version to be deleted is the ``backy.cfg`` option ::

    disallow_rm_when_younger_than_days: 6

However, instead of changing this option, you can simply use the ``--force``::

    $ backy2 rm 8fd42f1a-2364-11e7-8594-00163e8c0370 --force
        INFO: $ /usr/bin/backy2 rm 8fd42f1a-2364-11e7-8594-00163e8c0370 --force
        INFO: Removed backup version 8fd42f1a-2364-11e7-8594-00163e8c0370 with 10 blocks.
        INFO: Backy complete.

backy2 stores each block in the backup target (i.e. filesystem, s3, ...) once.
If it encounters another block on the backup source with the same checksum [1]_,
it will only write metadata which refers to the same backup target block.

So if a backup is deleted, backy2 needs to check if all references to each block
are gone. So backy2 can't just simply wipe all blocks from a removed backup
version.

As this is a resource-intensive task, it's separated into a special command::

    $ backy2 cleanup
        INFO: $ /usr/bin/backy2 cleanup
        INFO: Cleanup-fast: Cleanup finished. 0 false positives, 0 data deletions.
        INFO: Backy complete.

As you can see, nothing has been deleted. The reason for this is that backy2
is prepared to be used in parallel (backups during restores during scrubs).
On some edges (and this is one), it has timeouts to let data which might be
in the flow or in caches settle.

For cleanup this timeout is 1 hour. So you can now wait for 1 hour and repeat,
or you can use the alternative cleanup option (``-f``) which will ignore this
timeout. However be warned (also shown when doing a ``backy2 cleanup --help``):

    .. NOTE:: A full cleanup must not be run parallel to ANY other backy jobs.
        backy2 will prevent you from doing this by creating a global lock on the
        backup server.


.. [1] backy2 uses sha512 which can be configured in ``backy.cfg``. sha512
    is the only recommended checksum, however it's possible to use any other
    algorithm from python3's hashlib (i.e. ``md5``, ``sha1``, ``sha224``,
    ``sha256``, ``sha384``).
