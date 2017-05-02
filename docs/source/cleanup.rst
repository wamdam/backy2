.. include:: global.rst.inc

Cleanup
=======

In order to get rid of old backup versions, backy2 uses a garbage collection
algorithm which is a 2-step process in backy2.

Remove old versions
-------------------

In order to remove an old version, use ``backy2 rm``:

.. command-output:: backy2 rm --help

Example::

    $ backy2 rm 2453fe90-2f22-11e7-b961-a44e314f9270
        INFO: $ /home/dk/develop/backy2/env/bin/backy2 rm 2453fe90-2f22-11e7-b961-a44e314f9270
        INFO: Removed backup version 2453fe90-2f22-11e7-b961-a44e314f9270 with 25600 blocks.
        INFO: Backy complete.

There is a config option called ``disallow_rm_when_younger_than_days`` which
defaults to 6. If the to-be-removed backup version is younger than this value,
backy2 throws an error and exits with code 100::

    backy2 rm 2453fe90-2f22-11e7-b961-a44e314f9270
        INFO: $ /home/dk/develop/backy2/env/bin/backy2 rm 2453fe90-2f22-11e7-b961-a44e314f9270
       ERROR: Unexpected exception
       ERROR: 'Version 2453fe90-2f22-11e7-b961-a44e314f9270 is too young. Will not delete.'
       …
    backy2.backy.LockError: 'Version 2453fe90-2f22-11e7-b961-a44e314f9270 is too young. Will not delete.'
        INFO: Backy failed.

    $ echo $?
    100

Of course you can ``--force`` backy2 to delete this version::

    $ backy2 rm 2453fe90-2f22-11e7-b961-a44e314f9270 --force
        INFO: $ /home/dk/develop/backy2/env/bin/backy2 rm 2453fe90-2f22-11e7-b961-a44e314f9270 --force
        INFO: Removed backup version 2453fe90-2f22-11e7-b961-a44e314f9270 with 25600 blocks.
        INFO: Backy complete.


``backy2 rm`` removes version and corrosponding blocks from the metadata store.
It also adds the removed metadata entries into a *delete-candidates* - List.

In order to really delete blocks from the backup target, you'll need ``backy2
cleanup``.


backy2 cleanup
--------------

To free up space on the backup target, you need to cleanup.
There are two different cleanup methods, but you'll usually only need the
so-called *fast-cleanup*.

.. command-output:: backy2 cleanup --help

fast-cleanup
~~~~~~~~~~~~

``backy2 cleanup`` will go through the list of *delete-candidates* and check if
there are blocks which are not referenced from any other version anymore.

These blocks are then deleted from the backup-target. The still-in-use blocks
are removed from the list of *delete-candidates*.

In order to provide parallelism (i.e. multiple backy2 processes at the same
time), backy2 needs to prevent race-conditions between adding a
*delete-candidate* to the list and actually removing its data. That's why
a cleanup will only remove data blocks once they're on the list of
*delete-candidates* for more than 1 hour.

.. _full_cleanup:

full-cleanup
~~~~~~~~~~~~

There are times (e.g. when your database is corrupted or when you create a new
database based on export/import) when backy2 does not know if the blocks in the
backend storage are all known by the metadata store.

Then the ``-f`` option of cleanup comes into play. With this option, backy2 will
read all block UIDs from the backend storage (which can take *very* long) and
compare it to the list of known block UIDs in the metadata store.

Blocks unknown to the metadata store are then deleted from the backend storage.

Because this can be a very slow process, you can use the ``-p`` option to
provide a prefix. The first 10 characters of block UIDs are hexadecimal numbers
and small letters (0-9, a-f).

So a typical cleanup process after a desaster looks like this::

    $ backy2 cleanup -f -p 00
    $ backy2 cleanup -f -p 01
    …
    $ backy2 cleanup -f -p fe
    $ backy2 cleanup -f -p ff

Or even something like ::

    $ for p in $(printf %02x'\n' `seq -f %1.f 0 255`); do backy2 cleanup -f -p $p; done

