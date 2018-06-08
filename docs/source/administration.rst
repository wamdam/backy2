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
can restore this information with ``benji import-from-backend``.

You can also make further copies of the metadata with ``benji export``
and store them somewhere safe to increase your redundancy even more. It is
advisable to compress them as the JSON export format is quite verbose.

So now, even if your backup database server crashes, you'll still be able
to reimport all existing versions again.

.. NOTE:: **After** re-importing many versions, it is recommended to start a
    ``benji cleanup -f`` run as shown in section :ref:`full_cleanup`.

.. ATTENTION:: When you remove (``benji rm``) versions from the database and
    then call ``benji cleanup``, the blocks containing the backed up *data* will
    be removed. No ``benji import`` can bring them back, because Benji's export
    format *only* contains metadata information.

Database High-Availability
~~~~~~~~~~~~~~~~~~~~~~~~~~

An additional option against data loss is to replicate the SQL database. Please
refer to the database documentation. You should also have a regular database
backup in place.

.. CAUTION:: DBMS replication only helps when one server crashes or has a
    failure. It does not help against software-bug related data loss, human
    error and more. So ``benji export`` is the only reliable option for long-term
    data-safety.

Secure your block data
----------------------

Your *data backend* should be redundant in some way, too. Most cloud
providers have an SLA which guarantees a certain level of redundancy
and high-availability. If you manage your *data backend* yourself, then
you should look into redundancy technologies like:

- RAID 1, 5 and 6
- Redundancy provided by a distributed objected store like Ceph or Minio
- DRBD
- Filesystem specific data redundancy and replication mechanisms in filesystem
    like Btrs or ZFS

If your *data backend* fails or has corruptions, at best corrupted restores will
be possible. Benji does not store any redundant data and it cannot  restore
data from stored checksums alone.

Monitoring
----------

Tips & tricks
~~~~~~~~~~~~~

You should monitor exit codes of Benji closely. Anything != 0 means: There was
a problem.

Benji writes all output including possible tracebacks and command lines to
the configured logfile (see :ref:`configuration`).
If anything goes wrong, you'll be able to visit this logfile and get
enough information to troubleshoot the problem, even if this Benji call
came from an automated script.

You should also monitor the success of the backups. In addition to checking the
exit code, you can do this with ``benji ls`` and see if the column ``valid``
is True. This will be True as soon as the backup has finished successfully.

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
    +---------------------+-------------+------+---------------+----------+------------+------------+-------------+---------------+----------------+-------------+--------------+--------------+---------------+--------------+
    |         date        |     uid     | name | snapshot_name |   size   | block_size | bytes read | blocks read | bytes written | blocks written | bytes dedup | blocks dedup | bytes sparse | blocks sparse | duration (s) |
    +---------------------+-------------+------+---------------+----------+------------+------------+-------------+---------------+----------------+-------------+--------------+--------------+---------------+--------------+
    | 2018-06-07T12:51:20 | V0000000001 | test |               | 41943040 |  4194304   |   41943040 |          10 |      41943040 |             10 |           0 |            0 |            0 |             0 |           0s |
    | 2018-06-08T12:26:53 | V0000000002 | test |               | 41943040 |  4194304   |   41943040 |          10 |      41943040 |             10 |           0 |            0 |            0 |             0 |           0s |
    | 2018-06-08T12:26:56 | V0000000003 | test |               | 41943040 |  4194304   |   41943040 |          10 |             0 |              0 |    41943040 |           10 |            0 |             0 |           0s |
    | 2018-06-08T12:26:58 | V0000000004 | test |               | 41943040 |  4194304   |   41943040 |          10 |             0 |              0 |    41943040 |           10 |            0 |             0 |           0s |
    +---------------------+-------------+------+---------------+----------+------------+------------+-------------+---------------+----------------+-------------+--------------+--------------+---------------+--------------+

Machine output
~~~~~~~~~~~~~~


Some commands (like ``ls``, ``stats``, ``backup`` and ``enforce``) can also produce
machine readable JSON output for usage in scripts::

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

``jq`` is an excellent tool for parsing this data and filtering out the bits you
want. Here's a short example, but see the ``scripts/`` and ``images/benji-rook/scripts``
directories for more ones::

    $ benji -m ls | jq -r '.versions[0].date'
    2018-06-07T12:51:19

With machine readable output you can use the option ``--include-blocks``
to ``ls`` which also includes all blocks of this version in the output.

Version UIDs will be represented as simple integers without V prefix
and being zero-filled. All Benji commands are able to take this
representation as well, so you can use it in further commands as-is.

All timestamps are in UTC and without timezone information.

Debugging
~~~~~~~~~

In case something goes wrong, you can use the ``-v`` switch to increase the
logging verbosity. This outputs much more information.
