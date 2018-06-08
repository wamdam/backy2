.. include:: global.rst.inc

.. _scrubbing:

Scrub
=====

Scrubbing backups is needed to ensure data consistency.

.. command-output:: benji scrub --help

Reasons for Scrubbing
---------------------

Benji backs up data in blocks. These blocks are referenced from the metadata
store. When restoring images, these blocks are read and restored in the order
the metadata store says. As Benji also does deduplication, an invalid block
could potentially create invalid restore data on multiple places.

Invalid blocks can happen in these cases (probably incomplete):

- Bit rot / Data degradation (https://en.wikipedia.org/wiki/Data_degradation)
- Software failure when writing the block for the first time
- OS errors and bugs
- Human error: deleting or modifying blocks by accident
- Software errors in Benji and other used tools

Scrubbing Methods
-----------------

Benji implements three different scrubbing methods:

Consistency and Checksum
~~~~~~~~~~~~~~~~~~~~~~~~

::

    benji deep-scrub <version_uid>

Benji reads block-metadata (UID and checksum) from the metadata store, reads
the block by its UID from the *data backend*, calculates its checksum and
compares the checksums.

Using the Backup Source
~~~~~~~~~~~~~~~~~~~~~~~

::

    benji deep-scrub -s <snapshot> <version_uid>

Benji also reads the backup source for this version. This means that
Benji reads the block metadata (UID, position and checksum), reads the
corresponding source data block, the target block, calculates the checksum
of both, compares these checksums to the stored one and compares the source- and
data-block byte for byte.

This is not necessarily slower, but it will of course create some load on the
source storage, whereas target-only scrub only creates load on the target
storage.

Consistency Only
~~~~~~~~~~~~~~~~

::

    benji scrub <version_uid>


Benji only checks the metadata consistency between the metadata saved in the
database and the metadata accompanying each block. It also checks if the
block exists and has the right length. The actual data is **not** checked.

This mode of operation can be a useful in addition to deep-scrubs if
you pay for data downloads or bandwidth is limited. It is not a replacement
for doing deep-scrubs but you can reduce their frequency.

What Scrubbing Does Not
-----------------------

- Scrubbing doesn't modify any block data on the *data backend*

This means it

- doesn't fix filesystem errors on backed up images
- doesn't check for any structure within blocks
- and doesn't replay database journals or execute similar repair operations.

Scrubbing Failures
------------------

If scrubbing finds invalid blocks, these blocks are marked as *invalid*
in the metadata store. However, such blocks **will persist** and not be deleted.

Also, the versions affected by such invalid blocks are marked *invalid*.
Such versions cannot be the base (i.e. ``benji backup -f``, see
:ref:`differential_backup`) for differential backups anymore, Benji will throw
an error if you try.

However, invalid versions **can still be restored**. So a single block will not
break the restore process. Instead, you'll get a clear log output that there
is invalid data restored.

You can find invalid versions by looking at the output of ``benji ls``::

    $ benji  ls
        INFO: $ benji ls
    +---------------------+-------------+------+---------------+----------+------------+-------+-----------+------+
    |         date        |     uid     | name | snapshot_name |     size | block_size | valid | protected | tags |
    +---------------------+-------------+------+---------------+----------+------------+-------+-----------+------+
    | 2018-06-07T12:51:19 | V0000000001 | test |               | 41943040 |    4194304 | False |   False   |      |
    +---------------------+-------------+------+---------------+----------+------------+-------+-----------+------+


.. NOTE:: Multiple versions can be affected by a single block as Benji does
    deduplication and one block can belong to multiple versions, even to
    different images.


Partial Scrubbing
-----------------

If scrubbing all your backups creates too much load or takes too long, you can
use the ``-p`` parameter. With this parameter, Benji performs a
*partial scrub*. It will statistically (i.e. by random) choose the given
percentage of existing blocks in the version and scrub only these.

So if you call::

    benji deep-scrub -p 15 <version_uid>

each day for each version, you'll have statistically scrubbed 105% of all blocks
after seven days.
