.. include:: global.rst.inc

Cleanup
=======

In order to get rid of old backup versions, benji uses a garbage collection
algorithm which is a 2-step process in benji.

Remove old versions
-------------------

In order to remove an old version, use ``benji rm``:

.. command-output:: benji rm --help

Example::

    $ benji rm 2453fe90-2f22-11e7-b961-a44e314f9270
        INFO: $ /home/dk/develop/benji/env/bin/benji rm 2453fe90-2f22-11e7-b961-a44e314f9270
        INFO: Removed backup version 2453fe90-2f22-11e7-b961-a44e314f9270 with 25600 blocks.
        INFO: Benji complete.

There is a config option called ``disallow_rm_when_younger_than_days`` which
defaults to 6. If the to-be-removed backup version is younger than this value,
benji throws an error and exits with code 100::

    benji rm 2453fe90-2f22-11e7-b961-a44e314f9270
        INFO: $ /home/dk/develop/benji/env/bin/benji rm 2453fe90-2f22-11e7-b961-a44e314f9270
       ERROR: Unexpected exception
       ERROR: 'Version 2453fe90-2f22-11e7-b961-a44e314f9270 is too young. Will not delete.'
       …
    benji.backy.LockError: 'Version 2453fe90-2f22-11e7-b961-a44e314f9270 is too young. Will not delete.'
        INFO: Benji failed.

    $ echo $?
    100

Of course you can ``--force`` benji to delete this version::

    $ benji rm 2453fe90-2f22-11e7-b961-a44e314f9270 --force
        INFO: $ /home/dk/develop/benji/env/bin/benji rm 2453fe90-2f22-11e7-b961-a44e314f9270 --force
        INFO: Removed backup version 2453fe90-2f22-11e7-b961-a44e314f9270 with 25600 blocks.
        INFO: Benji complete.


``benji rm`` removes version and corrosponding blocks from the metadata store.
It also adds the removed metadata entries into a *delete-candidates* - List.

In order to really delete blocks from the backup target, you'll need ``benji
cleanup``.


benji cleanup
--------------

To free up space on the backup target, you need to cleanup.
There are two different cleanup methods, but you'll usually only need the
so-called *fast-cleanup*.

.. command-output:: benji cleanup --help

fast-cleanup
~~~~~~~~~~~~

``benji cleanup`` will go through the list of *delete-candidates* and check if
there are blocks which are not referenced from any other version anymore.

These blocks are then deleted from the backup-target. The still-in-use blocks
are removed from the list of *delete-candidates*.

In order to provide parallelism (i.e. multiple benji processes at the same
time), benji needs to prevent race-conditions between adding a
*delete-candidate* to the list and actually removing its data. That's why
a cleanup will only remove data blocks once they're on the list of
*delete-candidates* for more than 1 hour.

.. _full_cleanup:

full-cleanup
~~~~~~~~~~~~

There are times (e.g. when your database is corrupted or when you create a new
database based on export/import) when benji does not know if the blocks in the
backend storage are all known by the metadata store.

Then the ``-f`` option of cleanup comes into play. With this option, benji will
read all block UIDs from the backend storage (which can take *very* long) and
compare it to the list of known block UIDs in the metadata store.

Blocks unknown to the metadata store are then deleted from the backend storage.

Because this can be a very slow process, you can use the ``-p`` option to
provide a prefix. The first 10 characters of block UIDs are hexadecimal numbers
and small letters (0-9, a-f).

So a typical cleanup process after a desaster looks like this::

    $ benji cleanup -f -p 00
    $ benji cleanup -f -p 01
    …
    $ benji cleanup -f -p fe
    $ benji cleanup -f -p ff

Or even something like ::

    $ for p in $(printf %02x'\n' `seq -f %1.f 0 255`); do benji cleanup -f -p $p; done

