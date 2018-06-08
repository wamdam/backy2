.. include:: global.rst.inc

Cleanup
=======

In order to remove of old backup versions, Benji uses a two step garbage
collection algorithm.

Removing old versions
---------------------

In order to remove an old version, use ``benji rm``:

.. command-output:: benji rm --help

Example::

    $ benji rm -f V1
        INFO: $ benji rm -f V1
        INFO: Removed version V0000000001 metadata from backend storage.
        INFO: Removed backup version V0000000001 with 10 blocks.

There is a config option called ``disallowRemoveWhenYounger`` which
defaults to 6. If you request the removal of a backup version that is younger
than this value, Benji exists with an error::

    $ benji rm V1
           INFO: $ benji rm V1
          ERROR: Version V0000000001 is too young. Will not delete.

You can force the removal of a version by using ``--force``.

``benji rm`` removes the version metadata and corresponding blocks from the
*metadata backend*. It also adds the removed block entries into a deletion
candidate list.

In order to really delete blocks from the *data backend*, you'll need ``benji
cleanup``.

benji cleanup
-------------

To free up space on the *data backend*, you need to cleanup.
There are two different cleanup methods, but you'll usually only need the
so-called *fast-cleanup*.

.. command-output:: benji cleanup --help

fast-cleanup
~~~~~~~~~~~~

``benji cleanup`` will go through the list of deletion candidates and check if
there are blocks which aren't referenced from any other version anymore.

These blocks are then deleted from the *data backend*. The still-in-use blocks
are removed from the list of candidates.

In order to provide parallelism (i.e. multiple Benji processes at the same
time), Benji needs to prevent race-conditions between removing a block
completely and referencing this block from another version. That is why
a cleanup will only remove data blocks once they're on the list of
deletion condidates for more than one hour.

.. _full_cleanup:

full-cleanup
~~~~~~~~~~~~

There are times (e.g. when your database is corrupted or when you create a new
database based on export/import) when Benji does not know if the blocks in the
*data backend* are all known by the metadata store.

Then the ``-full`` option of cleanup comes into play. With this option, Benji will
read all block UIDs from the backend storage (which can take *very* long) and
compare it to the list of known block UIDs in the *metadata backend*

Blocks unknown to the *metadata backend* are then deleted from the *data backend*.

