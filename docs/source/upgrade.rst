.. include:: global.rst.inc

Upgrade
=======

Metadata
~~~~~~~~

Upgrading backy2 is done within backy2. Usually you don't have to do anything
as backy2 handles upgrades (metadata and backup data) by itself. Still, here
are some details on the process.

After upgrading, whenever backy2 is called with any command, backy2 checks the
metadata version of your database and starts an upgrade process if necessary.

.. NOTE::
   Under the hood backy2 uses alembic for the hard work on the metadata store.

You may upgrade backy2 even while jobs are running. The next job will upgrade
(and block!) the database until the upgrade is done. So the here's the recommended
process:

1. Let all jobs finish, don't start new ones.
2. Run ``backy2 ls`` and wait until the upgrade is done (no output will be produced).

PostgreSQL
----------

We use postgresql ourselves in our datacenter and therefore postgresql migrations
are very well tested. Usually there should be nothing else to do for
postgresql metadata storage on upgrades.


sqlite
------

We do not test upgrades on sqlite. Especially as sqlite does not support
``alter table`` for existing columns these upgrades may fail.
However the failure should not touch or even destroy your backup data. It will
just throw an exception and exit.

The recommended process for sqlite upgrades is:

 1. Export all your backups via ``backy2 export``
 2. Move your sqlite database away (i.e. start freshly)
 3. Import all your exports again

mysql
-----

We have not tested backy2 with mysql. Upgrade procedures like with sqlite
should work however.

Backup data
~~~~~~~~~~~

backy2 can upgrade your backup data (i.e. *blocks*) on demand. This can
happen the first time on backy2 V. 2.12.1 as encryption and compression have
been introduced.

If you set ``encryption_version: 0`` into your ``backy.cfg`` file,
nothing will change. If you set ``encryption_version: 1`` (and a
corrosponding encryption_key), backy2 will store *new* blocks encrypted
and compressed but will leave old blocks untouched. So only newly stored
data will be encrypted.

Please note especially that deduplication will continue to use old, i.e.
unencrypted blocks.

If you want to use encryption and compression you will need to migrate
your data to encrypted blocks. See :ref:`migrate-encryption`.

.. NOTE::
   This works because the encryption version is stored in each block's
   metadata along with the encryption key.

