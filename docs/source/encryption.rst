.. include:: global.rst.inc

Encryption
==========

In this chapter you will learn everything about backy2's encryption, migration to
the next encryption version and changing your keys (aka re-keying)

.. command-output:: backy2 migrate-encryption --help
.. command-output:: backy2 rekey --help


Configuration
-------------

You must set a encryption key (64 hex characters, resulting in a 32-byte AES key)
to the ``[DEFAULTS]`` section of backy.cfg. You will only be able to read or scrub
data when the key is correct (or the blocks don't have encryption)::

   encryption_key: decafbaddecafbaddecafbzddecafbaddecafbaddecafbaddecafbaddecafba
   encryption_version: 1

.. NOTE::
   The above key is intentionally invalid so that nobody copy&pastes this.
   
Please create your own key, e.g. via::

    $ openssl rand -hex 32


If you lose your key
--------------------

If you lose your key you will not be able to restore or scrub your data and you will
have to backup all data again. Please note that if you lose your key, you will have to
start with a new backy2 database.

The reason is, that backy2 *will not* check if existing blocks (which may be
used when using a hints-file during backup or when deduplication is enabled)
have a valid encryption key during backups for performance reasons.


Re-Keying
---------

Re-keying is good habit. You may change your backup key every now and then, for
example when persons leave your company and they might have the old key.

Re-keying *does not* access your data backend (i.e. the stored blocks). This
means, it's a relatively fast, local process on your backup-server.

This is possible because backy2 sets an individual encryption key for each
block and wraps this key with the key in your ``backy2.cfg``. When re-keying,
the encryption-key will not be changed. Instead, it will be unwrapped with your
old key and wrapped with your new key. As the wrapped keys are stored in your
meta backend (i.e. your database), this action can be performed locally.

These are the recommended steps for re-keying:

 1. check that you have ``backy2 export`` for every of your versions, because the export contains the keys wrapped by your old key
 2. comment out your old key configured in ``encryption_key`` in ``backy.cfg``
 3. create a new key by calling ``openssl rand -hex 32``
 4. set the new key into ``backy.cfg`` in ``encryption_key``
 5. call ``backy2 rekey <oldkey>``
 6. wait

The rekeying is done in one database transaction and it will lock backy2
completely (i.e. you can not run any backups, scrubs, restores while it
is running).

Just in case your DBMS has a bug or something crashes really badly there's a
small chance that this process only changes some blocks. If this happens,
remove all existing version (``backy2 rm -f <version>``) and re-import
them from the exports you created in step 1 above. Then repeat the process.

After the process has finished, create new exports for all versions because
they now contain the new wrapped keys.


Encryption versions
-------------------

backy2 defines integer versions for encryption. Usually 1 should be better
than 0, 2 better than 1.

As of this writing, version 1 is the current version whereas previous backy2
versions created backups with no encryption, which has been redefined as
encryption version 0.

Version 1 uses AES GCM with 256bit keys + a nonce per block. Before it
encrypts a block, data is compressed with lz4 compression level 1.

If we learn that there's a problem with this encryption, we will be able
to implement a next version with corrected encryption or compression.


Migrating to the next encryption version
----------------------------------------

When migrating to a new encryption version, you will have to call
``backy2 migrate-encryption <version>`` for each version.

backy2 will then create *a new version* for the encrypted blocks. Doing
this means, backy2 will read each block from the data backend, decrypt it,
encrypt it and store it to the data backend again, just like a regular
backup.

However ``migrate-encryption`` is faster than restoring and backing up
again because it benefits from existing blocks which already are
encrypted - just like deduplication works.

These are the steps to perform in order to migrate to the next encryption
version (if available).

 1. set the new ``encryption_version`` in backy.cfg
 2. for each existing version (``backy2 ls -f uid``) call ``backy2 migrate-encryption <version uid>``

This process may run in parallel to running backups/restores.


