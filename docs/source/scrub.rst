.. include:: global.rst.inc

.. _scrubbing:

Scrub
=====

Scrubbing backups is needed to ensure data consistency over time.





Reasons for Scrubbing
---------------------

Benji backs up data in blocks. These blocks are referenced from the metadata
store. When restoring images, these blocks are read and restored in the order
the metadata store says. As Benji also does deduplication, an invalid block
could potentially create invalid restore data in multiple places.

Invalid blocks can happen in these cases (probably incomplete):

- Bit rot / Data degradation (https://en.wikipedia.org/wiki/Data_degradation)
- Software failure when writing the block for the first time
- OS errors and bugs
- Human error: deleting or modifying blocks by accident
- Software errors in Benji and other used tools

Scrubbing Methods
-----------------

Benji implements three different scrubbing methods. Each of these methods
accepts the ``--block-percentage`` (short form ``-p``) option. With it you
can limit the scrubbing to a randomly selected percentage of the blocks.

.. ATTENTION:: When using the ``--block-percentage`` option with a value of
    less than 100 percent with any of the deep scrubbing commands, an invalid
    *version* won't be marked as valid again, when it has been marked as
    invalid in the past. Only a full successful deep-scrub will do that.

Consistency and Checksum
~~~~~~~~~~~~~~~~~~~~~~~~

.. command-output:: benji deep-scrub --help

Benji reads block-metadata (UID and checksum) from the metadata store, reads
the block by its UID from the *data backend*, calculates its checksum and
compares the checksums.

Using the Backup Source
~~~~~~~~~~~~~~~~~~~~~~~

::

    benji deep-scrub --source <snapshot> <version_uid>

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

.. command-output:: benji scrub --help

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

Bulk scrubbing
--------------

Benji also supports two commands to facilitate bulk scrubbing of versions:
``benji bulk-scrub`` and ``benji bulk-deep-scrub``:

.. command-output:: benji bulk-scrub --help
.. command-output:: benji bulk-deep-scrub --help

Both can take a list of *version* names. All *versions* matching these
names will be scrubbed. If you don't specify any names all *versions*
will be checked.

If the ``--tag`` (short form ``-t``) is given too, the above  selection is
limited to  *versions* also matching the given tag.  If  multiple ``--tag``
options are given, then they constitute an OR  operation.

By default all matching *versions* will be scrubbed. But you can also
randomly select a certain sample of these *versions* with ``--version-percentage``
(short form``-P``). A *version's* size isn't taken into account when selecting the
sample, every *version* is equally eligible.

The bulk scrubbing commands also accepts the ``--block-percentage`` (short
form ``-p``) option.

``benji bulk-deep-scrub`` doesn't support the ``--source`` option like
``benji deep-scrub``.

This is a good use cause for tags: You could mark your *versions* with a list of
different tags denoting the importance of the backed up data. Then you could scrub
each class of *versions* differently::

    # 14% of the versions are deep scrubbed for data of high importance
    $ benji bulk-deep-scrub --tag high --version-percentage 14

    # 7% of the versions are deep scrubbed for data of medium importance
    $ benji bulk-deep-scrub --tag medium --version-percentage 7

    # 3% of the versions are deep scrubbed for data of low importance
    $ benji bulk-deep-scrub --tag low --version-percentage 3

    # 3% of the versions are scrubbed when they contain reproducible bulk data
    $ benji bulk-scrub --tag bulk --version-percentage 3

If you'd call this schedule every day, you'd scrub the important data completely
about every seven days (statistically), data of medium importance completely every
fourteen days and low priority data completely every month. Bulk data would also
be scrubbed completely every month, but only metadata consistency and block
existence is checked.

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



