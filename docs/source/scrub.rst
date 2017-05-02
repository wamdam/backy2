.. include:: global.rst.inc

.. _scrubbing:

Scrub
=====

Scrubbing backups is needed to ensure data consistency.

.. command-output:: backy2 scrub --help

Why scrubbing is needed
-----------------------

backy2 backs up data in blocks. These blocks are referenced from the metadata
store. When restoring images, these blocks are read and restored in the order
the metadata store says. As backy2 also does deduplication, an invalid block
could potentially create invalid restore data on multiple places.

Invalid blocks can happen in these cases (probably incomplete):

- Bit rot / Data degradation: https://en.wikipedia.org/wiki/Data_degradation
- Software failure when writing the block for the first time
- CPU bug on backy2 server or target storage
- human error: deleting or modifying blocks
- backy2 software errors

What scrubbing does
-------------------

There are different scrubbing modes:

Target-only
~~~~~~~~~~~

When calling::

    backy2 scrub <version_uid>

backy2 reads block-metadata (UID and checksum) from the metadata store, reads
the block by it's UID from the target storage, calculates its checksum and
compares the checksums.

Backup Source based
~~~~~~~~~~~~~~~~~~~

When calling scrub with a source like::

    backy2 scrub -s <snapshot> <version_uid>

backy2 also reads the backup source for this version. This means that
backy2 reads the block metadata (UID, position and checksum), reads the
corrosponding source data block, the target block, calculates the checksum
of both, compares these checksums to the stored one and compares the source- and
data-block byte for byte.

This is not necessarily slower, but it will of course create some load on the
source storage, whereas target-only scrub only creates load on the target
storage.


What scrubbing does not
-----------------------

- modify block-data, so it does not:
- fix filesystem errors in backed-up images
- check any logic inside blocks
- replay journals or fix database files


When scrub finds invalid blocks
-------------------------------

If either scrubbing mode finds invalid blocks, these blocks are marked *invalid*
in the metadata store. However, such blocks **will persist** and not be deleted.

Also, the versions affected by such invalid blocks are marked *invalid*.
Such versions cannot be the base (i.e. backy2 backup -f, see
:ref:`differential_backup`) for differential backups anymore (backy2 will throw
an error if you try).

However, invalid versions **can still be restored**. So a single block will not
break the restore process. Instead, you'll get a clear log output that there
is invalid data restored.

You can find invalid versions by looking at the output of ``backy2 ls``::

    $ backy2 ls
        INFO: $ /home/dk/develop/backy2/env/bin/backy2 ls
    +------+------+---------------+------+------------+-----+-------+-----------+------+
    | date | name | snapshot_name | size | size_bytes | uid | valid | protected | tags |
    +------+------+---------------+------+------------+-----+-------+-----------+------+
    | …    | …    | …             | …    |          … | …   |   0   |     …     |      |
    +------+------+---------------+------+------------+-----+-------+-----------+------+
        INFO: Backy complete.

Invalid versions are shown with a ``0`` in the column ``valid``, valid versions
are shown with a ``1`` in this column.

.. NOTE:: Multiple versions can be affected by a single block as backy2 does
    deduplication and one block can belong to multiple versions, even to
    different images.


Partial scrubs
--------------

If scrubbing all your backups creates too much load or takes too long, you can
use the ``-p`` parameter from backy2. With this parameter, backy2 performs a
*partial scrub*. It will statistically (i.e. by random) choose the given
percentage of existing blocks in the version and scrub only these.

So if you call::

    backy2 scrub -p 15 <version_uid>

each day for each version, you'll have statistically scrubbed 105% of all blocks
after seven days.

