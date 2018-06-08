.. include:: global.rst.inc

Backup
======

In this chapter you will learn all possibilities and options for backup.

.. command-output:: benji backup --help


Simple backup
-------------

This is how you can create a normal backup::

    $ benji backup source name

where source is a URI and name is the name for the backup, which may contain any
quotable character.

.. NOTE:: The name and all other identifiers are stored in SQL VARCHAR
    columns which are created by SQLAlchemy's String type. Please refer to
    http://docs.sqlalchemy.org/en/latest/core/type_basics.html#sqlalchemy.types.String
    for reference.

The supported schemes for source are **file** and **rbd**. So these are
realistic examples::

    $ benji backup file:///var/lib/vms/database.img database
    $ benji backup rbd://poolname/database@snapshot1 database

Versions
--------

An instance of a backup is called a *version*. A version contains these metadata
fields:

* **date**: date and time of the backup, this is created by Benji.
* **uid**: unique identifier for this version, this is created by Benji
* **name**: name from the command line
* **snapshot_name**: snapshot name (option ``-s``) from the command line
* **size**: size of the backuped image in bytes
* **block_size**: block size in bytes
* **valid**: validity of this version (True or False)
  This is False while the backup for this version is running and will be set to True
  as soon as the backup has finished and all writers have flushed their data.
  Scrubbing may set this to False if the backup is found invalid for any reason.
* **protected**: indication if the version may be deleted (True or False)
* **tags**: list of (string) tags for this version

You can output this data with::

    $ benji ls
        INFO: $ benji ls
    +---------------------+-------------+------+---------------+----------+------------+-------+-----------+------+
    |         date        |     uid     | name | snapshot_name |     size | block_size | valid | protected | tags |
    +---------------------+-------------+------+---------------+----------+------------+-------+-----------+------+
    | 2018-06-07T12:51:19 | V0000000001 | test |               | 41943040 |    4194304 |  True |   False   |      |
    +---------------------+-------------+------+---------------+----------+------------+-------+-----------+------+


.. HINT::
    You can filter the output with various parameters:

    .. command-output:: benji ls --help

.. _differential_backup:

Differential backup
-------------------

Benji only backups changed blocks. It can do this in two different ways:

1. **It can read the whole image**: Checksum each block and look the checksum up
   in the metadata backend. If it is found, only a reference to the existing
   block will be stored, thus there's no write action on the data backend.

2. **It can receive a hints file**: The hints file is a JSON formatted list of
   (offset, size) tuples (see :ref:`hints_file` for an example) which indicate
   the status (used or changed) of each region of the image.
   Fortunately the format matches exactly the output of
   ``rbd diff … --format=json``.  In this case it will only read blocks hinted
   at by the *hints file*, checksum each block and look the checksum up in the
   metadata backend. If it is found (which may rarely happen for file copies
   or when blocks are all zeros), only a reference to the existing block will
   be stored. Otherwise the block is written to the data backend.
   The hints file is passed via the  ``-r`` or ``--rbd`` option to
   ``benji backup``.

.. NOTE:: Benji does **forward-incremental backups**. So in contrast to
    backward-incremental backups, there will never be any need to create another
    full backup after the first full backup.
    If you don't trust Benji, you are encouraged to use ``benji deep-scrub``,
    possibly with the ``[-s]`` parameter to see if the backup matches the source.

In any case, if the backup source changes size, Benji will assume that
the size change has happened at the end of the volume, which is the case if you
resize partitions, logical volumes or Ceph RBD images.


Examples of differential backups
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

LVM (or any other diff unaware storage)
***************************************

Day 1 (initial backup)::

    $ lvcreate --size 1G --snapshot --name snap /dev/vg00/lvol1
    $ benji backup file:///dev/vg00/snap lvol1
    $ lvremove -y /dev/vg00/snap

Day 2..n (differential backups)::

    $ lvcreate --size 1G --snapshot --name snap /dev/vg00/lvol1
    $ benji backup file:///dev/vg00/snap lvol1
    $ lvremove -y /dev/vg00/snap

.. IMPORTANT:: With LVM snapshots, the snapshot increases in size as the origin
    volume changes. If the snapshot is 100% full, it is lost and invalid.
    It is important to monitor the snapshot usage with the ``lvs`` command
    to make sure the snapshot doesn't fill up completely.
    The ``--size`` parameter defines the space reserved for changes during the
    snapshot's existence.

    Also note that LVM does read-write-write for any overwritten block while a
    snapshot exists. This may hurt your performance.

Ceph RBD
********

With Ceph RBD it's possible to let Ceph calculate the changes between two snapshots.
Since the *jewel* version of Ceph this is a very fast process if the *fast-diff*
feature is enabled. In this case only metadata has to be compared.


Manually
^^^^^^^^

In this example, we will backup an RBD image called ``vm1`` which is in the
pool ``pool``.

1. Create an initial backup::

    $ rbd snap create pool/vm1@backup1
    $ rbd diff --whole-object pool/vm1@backup1 --format=json > /tmp/vm1.diff
    $ benji backup -s backup1 -r /tmp/vm1.diff rbd://pool/vm1@backup1 vm1

2. Create a differential backup::

    $ rbd snap create pool/vm1@backup2
    $ rbd diff --whole-object pool/vm1@backup2 --from-snap backup1 --format=json > /tmp/vm1.diff

    # delete old snapshot
    $ rbd snap rm pool/vm1@backup1

    # get the uid of the version corrosponding to the old rbd snapshot. This
    # looks like "V001234567". Copy it.
    $ benji ls vm1 -s backup1

    # and backup
    $ benji backup -s backup2 -r /tmp/vm1.diff -f V001234567 rbd://pool/vm1@backup2 vm1

Automation
^^^^^^^^^^

This is how you can automate forward differential backups including automatic
initial backups where necessary::

.. literalinclude:: ../../scripts/ceph.sh

This is what it does:

* When the backup::ceph is called, it searches for the latest RBD
  snapshot. As RBD snapshots have no date assigned, it's the last one from
  a sorted output of ``rbd snap ls``.

.. NOTE:: Only RBD snapshots that begin the prefix *b-* are considered. All
    other snapshots are left alone. This makes it possible to have manual
    snapshots that aren't touched by Benji.

* If no RBD snapshot is found, an initial backup is performed.
* If there is an RBD snapshot, Benji is asked if it has a *version* of this
  snapshot. If not, an initial_backup is performed.
* If Benji has a *version* of this snapshot, a *hints file* is created via
  ``rbd diff --whole-object <new snapshot> --from-snap <old snapshot> --format=json``.
* Benji then only backups changes as listed in the *hints file*.

These functions could be called each day by a small script (or even multiple times
a day) and will automatically keep only one snapshot and create forward-differential
backups.

.. NOTE:: This alone won't be enough to be safe. You will have to perform
    additional scrubs. Please refer to section :ref:`scrubbing`.

Specifying a Block Size
-----------------------

To perform a backup Benji splits up the image into equal sized blocks.

.. NOTE:: Except the last block which may vary in length.

By default the block size specified in the configuration file is used.
But the block size can be changed on the command line on a version by version
basis, but be aware that this will affect deduplication and increase the space
usage.

One possible use case for different block sizes would be backing up LVM
volumes and Ceph images with the same Benji installation. While for Ceph 4MB
is usually the best size, LVM volumes might profit from a smaller block size.

If you want to base a new version on an old version (as it can be the case when
doing a differential backup) the block sizes of the old and new version
have to match. Benji will terminate with an error if that is not the case.

Tag Backups
-----------

A *version* can have multiple tags. They are just for use by the administrator
and have no function in Benji. To specify a tag the ``backup`` command provides
the command line switch ``-t`` or ``--tag``:

    $ benji backup -t mytag rbd://cephstorage/test_vm test_vm

You can also use multiple tags for one revision:

    $ benji backup -t mytag -t anothertag rbd://cephstorage/test_vm test_vm

Later on you can modify tags with the commands 'add-tag' and 'remove-tag':

    $ benji add-tag V0000000001 mytag
    $ benji rm-tag V0000000001 anothertag


Export Metadata
---------------

Benji has now backed up all image data to a (hopefully) safe place. However,
the blocks are of no use without the corresponding metadata. Benji
will need this information to get the blocks back in the correct order and
restore your image.

This information is stored in the *metadata backend*. Additionally Benji will
save the metadata on the *data backend* automatically. Should you lose your
*metadata backend*, you can restore these metadata backups by using
``benji import-from-backend``.

.. command-output:: benji import-from-backend --help

There is currently no mechanism to import the backup of all version's
metadata from the *data backend*, but you could get a list of all versions
manually from the *data backend*.

.. NOTE:: This metadata backup is compressed and encrypted like the blocks
    if you have these features enabled.

If you want to make your own copies of your metadata you can do so by using
``benji export``.

.. command-output:: benji export --help

If you're doing this programmatically and are exporting to STDOUT you should
probably add ``-m`` to your export command to reduce the logging level of Benji.

::

    $ benji -m export V1
    {
      "metadataVersion": "1.0.0",
      "versions": [
        {
          "uid": 1,
          "date": "2018-06-07T12:51:19",
          "name": "test",
          "snapshot_name": "",
          "size": 41943040,
          "block_size": 4194304,
          "valid": true,
          "protected": false,
          "tags": [],
          "blocks": [
            {
              "uid": {
                "left": 1,
                "right": 1
              },
              "date": "2018-06-07T14:51:20",
              "id": 0,
              "size": 4194304,
              "valid": true,
              "checksum": "aed3116b4e7fad9a3188f5ba7c8e73bf158dabec387ef1a7bca84c58fe72f319"
            },
    [...]

You can import such a dump of a version's metadata with ``benji import``.

.. command-output:: benji import --help

You can't import versions that already exist in the *metadata backend*.

.. _hints_file:

The Hints File
--------------

Example of a hints-file::

    [{"offset":0,"length":4194304,"exists":"true"},
    {"offset":4194304,"length":4194304,"exists":"true"},
    {"offset":8388608,"length":4194304,"exists":"true"},
    {"offset":12582912,"length":4194304,"exists":"true"},
    {"offset":16777216,"length":4194304,"exists":"true"},
    {"offset":20971520,"length":4194304,"exists":"true"},
    {"offset":25165824,"length":4194304,"exists":"true"},
    {"offset":952107008,"length":4194304,"exists":"true"}

.. NOTE:: The length may vary, however it's nicely aligned to 4MB when using
    ``rbd diff --whole-object``. As Benji by default also uses 4MB blocks,
    it will not have to recalculate which 4MB blocks are affected by more
    and smaller offset+length tuples (not that that'd take very long).
