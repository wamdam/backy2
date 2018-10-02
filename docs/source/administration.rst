.. _administration:

Administration
==============

Benji is an important tool when it's responsible for company-wide backups.
Backups, scrubs, restores and cleanups must run smoothly and need to be
monitored closely.

Also, as Benji has two parts (*metadata backend and *data backend*), both have to
be checked regularly and be as highly-available as possible.

.. _administration-meta-backend:

Secure your Metadata
--------------------

This section shows methods of how to keep your metadata safe even when
disasters happen.

Metadata Redundancy
~~~~~~~~~~~~~~~~~~~

Benji already exports the version metadata to the *data backend*, too. You
can restore this information with ``benji import-from-backend``:

.. command-output::benji import-from-backend --help

The metadata-backend-less uses this import from the *data backend* to
populate an in-memory database to enable restores when the metadata
backend is unavailable, please see section :ref:`metadata_backend_less`.

You can also make further copies of the metadata with ``benji export``
and store them somewhere safe to increase your redundancy even more. It is
advisable to compress them as the JSON export format is quite redundant.

.. command-output::benji export --help

You can import these exports again with:

.. command-output::benji import --help

If the imported *version* already exists in the *metadata backend* Benji
terminates with an error and doesn't proceed with the import.

So now, even if your backup database server crashes, you'll still be able
to reimport all existing versions again!

.. ATTENTION:: When you remove (``benji rm``) versions from the database and
    then call ``benji cleanup``, the blocks containing the backed up data will
    be removed. No ``benji import`` can bring them back, because Benji's export
    format *only* contains metadata information.

Database High-Availability
~~~~~~~~~~~~~~~~~~~~~~~~~~

An additional option against data loss is to replicate the SQL database. Please
refer to the database documentation. You should also have a regular database
backup in place.

.. CAUTION:: DBMS replication only helps when one server crashes or has a
    failure. It does not help against software-bug related data loss, human
    error and more. So the automatic metadata export and ``benji export`` are
    the only reliable options for long-term data safety.

Secure your block data
----------------------

Your *data backend* should be redundant in some way, too. Most cloud
providers have an SLA which guarantees a certain level of availability.
If you manage your *data backend* yourself, then you should look into
redundancy and high-availability technologies like:

- RAID 1, 5 and 6
- Redundancy provided by a distributed objected store like Ceph or Minio
- DRBD
- Filesystem specific data redundancy and replication mechanisms in filesystems
  like Btrs or ZFS

If your *data backend* fails or has corruptions, at best corrupted restores will
be possible. Benji doesn't store any redundant data and it cannot  restore
data from stored checksums alone.

Monitoring
----------

Tips & tricks
~~~~~~~~~~~~~

You should monitor exit codes of Benji closely. Anything != 0 means that there
was a problem.

Benji writes all output including possible tracebacks and command lines to
the configured logfile (see :ref:`configuration`).
If anything goes wrong, you'll be able to visit this logfile and get
enough information to troubleshoot the problem, even if Benji was called
from an automated script.

You should also monitor the success of the backups. In addition to checking the
exit code, you can do this with ``benji ls`` and see if the column ``valid``
is True. For a currently running backup this column is False but it will change
to True on successful completion of the backup.

You can also monitor the progress of the backups either by looking at the
logfile or by checking your process-tree::

    $ ps axfu|grep "[b]acky2"
    …  \_ benji [Scrubbing Version V00000001 (0.1%)]

To know which backup took how long and to see how many blocks/bytes have been
read and written, you can use the ``benji stats`` command:

.. command-output:: benji stats --help

Example::

    $ benji stats
        INFO: $ benji stats
    +---------------------+-------------+------+---------------+---------+------------+---------+---------+---------+--------+--------------+
    |         date        | uid         | name | snapshot_name |   size  | block_size |    read | written |   dedup | sparse | duration (s) |
    +---------------------+-------------+------+---------------+---------+------------+---------+---------+---------+--------+--------------+
    | 2018-06-13T15:21:55 | V0000000001 | test |               | 40.0MiB |   4.0MiB   | 40.0MiB | 40.0MiB |    0.0B |   0.0B |          00s |
    | 2018-06-13T15:21:57 | V0000000002 | test |               | 40.0MiB |   4.0MiB   | 40.0MiB |    0.0B | 40.0MiB |   0.0B |          00s |
    | 2018-06-13T15:21:58 | V0000000003 | test |               | 40.0MiB |   4.0MiB   | 40.0MiB |    0.0B | 40.0MiB |   0.0B |          00s |
    | 2018-06-13T15:21:59 | V0000000004 | test |               | 40.0MiB |   4.0MiB   | 40.0MiB |    0.0B | 40.0MiB |   0.0B |          00s |
    +---------------------+-------------+------+---------------+---------+------------+---------+---------+---------+--------+--------------+

.. _machine_output:

Machine output
~~~~~~~~~~~~~~

Some commands can also produce machine readable JSON output for usage in
scripts::

    $ benji -m ls
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
          "tags": []
        }
      ]
    }

.. NOTE:: Take care to put the ``-m`` between ``benji`` and ``ls``.

All messages emitted by Benji are written to STDERR. In contrast
the machine readable output is written to STDOUT. Also, when using ``-m`` the
logging level is adjusted to only output errors. The Benji logfile still gets
the whole output.

Here's a table of commands supporting machine readable output and their
output:

+-----------------+-----------------------------------------------------------+
| Command         | Description of output                                     |
+=================+===========================================================+
| ls              | List of matching *versions*                               |
+-----------------+-----------------------------------------------------------+
| stats           | List of matching statistics                               |
+-----------------+-----------------------------------------------------------+
| backup          | List of newly create *version*                            |
+-----------------+-----------------------------------------------------------+
| enforce         | List of removed *versions*                                |
+-----------------+-----------------------------------------------------------+
| scrub           | List of scrubbed *versions* and of *versions* with errors |
+-----------------+-----------------------------------------------------------+
| deep-scrub      | List of scrubbed *versions* and of *versions* with errors |
+-----------------+-----------------------------------------------------------+
| bulk-scrub      | List of scrubbed *versions* and of *versions* with errors |
+-----------------+-----------------------------------------------------------+
| bulk-deep-scrub | List of scrubbed *versions* and of *versions* with errors |
+-----------------+-----------------------------------------------------------+

All other commands also accept the ``-m`` switch. But for them only the logging
level is turned down.

`jq <https://stedolan.github.io/jq/>`_ is an excellent tool for parsing this data
and filtering out the bits you want. Here's a short example, but see the ``scripts/``
and ``images/benji-rook/scripts/`` directories for more::

    $ benji -m ls | jq -r '.versions[0].date'
    2018-06-07T12:51:19

With machine readable output you can use the option ``--include-blocks``
to ``ls`` which includes all blocks of this version in the output.

Version UIDs will be represented as simple integers without the V prefix
and being zero-filled. All Benji commands are able to take this
representation as well, so you can use such UIDs in further commands as-is.

All timestamps are in UTC and without timezone information.

Debugging
~~~~~~~~~

In case something goes wrong, you can use the ``-v`` switch to increase the
logging verbosity. This outputs much more information.
