.. _administration-guide:

backy2 Administration guide
===========================

backy2 is an important tool when it's responsible for company-wide backups.
Backups, scrubs, restores and cleanups must run smoothly and need to be
monitored closely.

Also, as backy has two parts (metadata store and data backend), both have to
be checked regularily and be as highly-available as possible.

.. _administration-guide-meta-storage:

Secure the meta backend storage
-------------------------------

This section shows methods how to make the metadata store available even when
disasters happen.

backy2 export
~~~~~~~~~~~~~

The recommended way of keeping metadata along with your data is ``backy2
export``:

.. command-output:: backy2 export --help

The *export* command will write metadata for a specific version into a (CSV)
file. This is roughly how this looks like::

    backy2 Version 2.2 metadata dump
    d91be794-2f21-11e7-b961-a44e314f9270,2017-05-02 10:26:48,test,,25600,104857600,1,0
    6ea578608ffuwQB2rhRMMevpJtVrNU7a,d91be794-2f21-11e7-b961-a44e314f9270,0,2017-05-02 12:26:51,04ca5d5da5270cf1e6a2ce09afc854a959eec7d59198b76436d3c40075b77f498d27d0891bdee01ccda017073390c150c01001b1c5e8289961c7a798a51a8964,4096,1
    d63675c78fMfgq9ULna3NwLBFvLNhy27,d91be794-2f21-11e7-b961-a44e314f9270,1,2017-05-02 12:26:51,bf80fb0bb63f1c79af7196ac8d5c0831c3fb9f1e532b2d190567a1351a689687b6892ae00d24a2db69d1a6f167670e2c34ddd81d4f453e934f7901df6f35f9f9,4096,1
    0cb2d82e64eg4eCMNixT79HpfEJnbZTB,d91be794-2f21-11e7-b961-a44e314f9270,2,2017-05-02 12:26:51,d619455cb43df5a7a5426ba1020ee47a79bd3ed0d0de977dbd99350569d4dff5647fcb9380a70e729d7891cc67a6f16a424a38ec1f1794097334091fb7a606ed,4096,1

.. NOTE:: backy2 tries very hard to support older versioned export data in
    newer versions.

After each backup you should export the generated version and store it together
with your backup data (that is in the same backup storage like NFS, S3, ...).

So even if your backup database server crashes, you'll still be able to reimport
all existing versions later.

.. NOTE:: **After** re-importing many versions, it is recommended to start a
    ``backy2 cleanup -f`` run as shown in section :ref:`full_cleanup`.

.. ATTENTION:: When you remove (``backy2 rm``) versions from the database and
    then call ``backy2 cleanup``, the blocks containing the backed up *data* will
    be removed. No ``backy2 import`` can bring them back, because backy2's export
    format *only* contains metadata information.


backy2 import
~~~~~~~~~~~~~

In order to get an exported version back, you must import it again:

.. command-output:: backy2 import --help

Example::

    $ backy2 import myvm.20170421.backy2

This will reimport with the same version UID as it had before (as this is stored
in the export file). backy2 will not allow to import a version UID which already
is in the database.


SQL high availability
~~~~~~~~~~~~~~~~~~~~~

An additional option against data loss is to mirror the sql database. All usual
mirroring techniques apply here. Please look into your database documentation.

.. CAUTION:: DBMS mirroring only helps when one server crashes or has a
    failure. It does not help against software-bug related data loss, human
    error and more. So backy2 export is the only reliable option for long-term
    data-safety.


High available data backend
---------------------------

Your data backend should be redundant in some way too. Examples are:

- RAID 1, 5, 6
- Redundant S3 compatible storage (riak cs, ceph object gateway, …)
- DRBD
- Some zfs mirroring should work too

If your data backend fails or has corruptions, at best corrupted restores will
be possible. backy2 does not store any redundant data neither can it restore
data from stored checksums.


Monitoring
----------

Tips & tricks
~~~~~~~~~~~~~

You should monitor exit codes of backy closely. Anything != 0 means: There was
a problem.

backy2 writes all output including possible tracebacks and command lines to
the logfile configured in backy.cfg (see :ref:`config_file`).
If anything goes wrong, you'll be able to visit this logfile and get
output, even if this backy2 call came from an automated script.

You should also monitor success of the backups. In addition to checking the
exit code, you can do this via ``backy2 ls`` and see if the column ``valid``
is 1. This will be 1 as soon as the backup has finished successfully.

You can also monitor progress of the backups either by looking at the mentioned
logfile or by checking your process-tree::

    $ ps axfu|grep "[b]acky2"
    …  \_ backy2 [Scrubbing test (9054672e-7e3e-11ea-a694-003048d74f6c) Read Queue [          ] Write Queue [          ] (2.0% 2.4MB/s ETA 83s)]

To know which backup took how long and to see how many blocks/bytes have been
read and written, you can use the excellent ``backy2 stats`` command:

.. command-output:: backy2 stats --help

Example::

    $ backy2 stats -l3
        INFO: $ /home/dk/develop/backy2/env/bin/backy2 stats -l3
    +---------------------+--------------------------------------+-------+------------+-------------+------------+-------------+---------------+----------------+-------------+--------------+--------------+---------------+--------------+
    |         date        |                 uid                  | name  | size bytes | size blocks | bytes read | blocks read | bytes written | blocks written | bytes dedup | blocks dedup | bytes sparse | blocks sparse | duration (s) |
    +---------------------+--------------------------------------+-------+------------+-------------+------------+-------------+---------------+----------------+-------------+--------------+--------------+---------------+--------------+
    | 2017-04-12 20:28:03 | 832dc202-1fbe-11e7-9f25-a44e314f9270 | small |   10485760 |        2560 |   10485760 |        2560 |      10485760 |           2560 |           0 |            0 |            0 |             0 |            7 |
    | 2017-04-12 20:28:26 | 90fbbeb6-1fbe-11e7-9f25-a44e314f9270 | small |   10485760 |        2560 |   10485760 |        2560 |             0 |              0 |    10485760 |         2560 |            0 |             0 |            7 |
    | 2017-05-02 10:27:48 | d91be794-2f21-11e7-b961-a44e314f9270 | test  |  104857600 |       25600 |  104857600 |       25600 |        323584 |             79 |   104534016 |        25521 |            0 |             0 |           60 |
    +---------------------+--------------------------------------+-------+------------+-------------+------------+-------------+---------------+----------------+-------------+--------------+--------------+---------------+--------------+
        INFO: Backy complete.

To find out which backup takes up how much space, you can use the ``backy2 du`` command:

.. command-output:: backy2 du --help

Here's an example and I'm trying to describe the meaning of the output::

    backy2 du 30d53cea-7ff8-11ea-9466-8931a4889813
       INFO: $ backy2 du 30d53cea-7ff8-11ea-9466-8931a4889813
   +-------------+-----------+-----------+--------------+------------+------------+------------------+
   |        Real |      Null | Dedup Own | Dedup Others | Individual | Est. Space | Est. Space freed |
   +-------------+-----------+-----------+--------------+------------+------------+------------------+
   | 21323841536 | 150994944 |         0 |  20774387712 |  549453824 | 3600314339 |        549453824 |
   +-------------+-----------+-----------+--------------+------------+------------+------------------+
       INFO: Backy complete.

Real
   The size of the version in bytes when restored.

Null
   The number of bytes (4MB-block-wise) that are \\0 in this version.
   These are not stored in the backup target, instead they're only
   referenced in the metadata so they take up virtually no space.

Dedup Own
   Bytes (again 4MB-block-wise) which are deduplicated within this
   version and nowhere else.

Dedup Others
   Bytes (again 4MB-block-wise) which are duplicates also found in
   other versions.

Individual
   Bytes that are specific to this version (no duplicates in other versions)

Est. Space
   From the former values a calculated byte-size how much space this
   version takes up. The calculation divides duplicate (=shared) blocks
   by the number they occur in other versions +1 (for this version).
   Unshared blocks are just added, Null blocks are not added.

Est. Space freed
   Estimated space freed on the target storage when this version is
   deleted.


If you don't like byte-values, just use the ``-r`` switch for backy2::

    backy2 du 30d53cea-7ff8-11ea-9466-8931a4889813
       INFO: $ backy2 du 30d53cea-7ff8-11ea-9466-8931a4889813
   +--------+---------+-----------+--------------+------------+------------+------------------+
   |   Real |    Null | Dedup Own | Dedup Others | Individual | Est. Space | Est. Space freed |
   +--------+---------+-----------+--------------+------------+------------+------------------+
   | 20 GiB | 144 MiB |         0 |       19 GiB |    524 MiB |      3 GiB |          524 MiB |
   +--------+---------+-----------+--------------+------------+------------+------------------+
       INFO: Backy complete.


Machine output
~~~~~~~~~~~~~~

All commands in backy2 are available with machine compatible output too.
Columns will be pipe (``|``) separated.

Example::

    $ backy2 -m ls
    type|date|name|snapshot_name|size|size_bytes|uid|valid|protected|tags
    version|2017-04-18 18:05:04.174907|vm1|2017-04-19T11:12:13|25600|107374182400|c94299f2-2450-11e7-bde0-003048d74f6c|1|0|b_daily,b_monthly,b_weekly

    $ backy2 -m stats -l3
    type|date|uid|name|size bytes|size blocks|bytes read|blocks read|bytes written|blocks written|bytes dedup|blocks dedup|bytes sparse|blocks sparse|duration (s)
    statistics|2017-04-12 20:28:03|832dc202-1fbe-11e7-9f25-a44e314f9270|small|10485760|2560|10485760|2560|10485760|2560|0|0|0|0|7
    statistics|2017-04-12 20:28:26|90fbbeb6-1fbe-11e7-9f25-a44e314f9270|small|10485760|2560|10485760|2560|0|0|10485760|2560|0|0|7
    statistics|2017-05-02 10:27:48|d91be794-2f21-11e7-b961-a44e314f9270|test|104857600|25600|104857600|25600|323584|79|104534016|25521|0|0|60

With machine output, the log-level of backy2 is reduced to *WARNING*, no matter
what backy.cfg says.

.. HINT::
    Pipe separated content can be read easily with awk::

        awk -F '|' '{ print $3 }'

.. HINT::
    For simplicity you can skip the header with the ``-s`` switch::

        $ backy2 -ms ls

In ``ls`` machine output also accepts a fields list via ``--fields`` or ``-f``. For example you could output
only the uid column skipping headers in machine output::

    $backy2 -ms ls -f uid

Or if you want to output date, name, valid and tags e.g. for monitoring::

    $backy2 -ms ls -f date,name,valid,tags


Debugging
~~~~~~~~~

In case anything goes wrong, you may use the DEBUG log-level. You can temporarily
enable this with the generic ``-v`` switch::

    $ backy2 -v ls
        INFO: $ /home/dk/develop/backy2/env/bin/backy2 -v ls
       DEBUG: backup.ls(**{'tag': None, 'snapshot_name': None, 'name': None})
    +---------------------+---------------+--------------------------------------+-------+------------+--------------------------------------+-------+-----------+----------------------------+
    |         date        | name          | snapshot_name                        |  size | size_bytes |                 uid                  | valid | protected | tags                       |
    +---------------------+---------------+--------------------------------------+-------+------------+--------------------------------------+-------+-----------+----------------------------+
    | 2017-05-02 10:42:02 | copy on write | d91be794-2f21-11e7-b961-a44e314f9270 | 25600 |  104857600 | fa196d8e-2f23-11e7-b961-a44e314f9270 |   1   |     0     |                            |
    | 2017-05-02 10:26:48 | test          |                                      | 25600 |  104857600 | d91be794-2f21-11e7-b961-a44e314f9270 |   1   |     0     | b_daily,b_monthly,b_weekly |
    +---------------------+---------------+--------------------------------------+-------+------------+--------------------------------------+-------+-----------+----------------------------+
       DEBUG: Writer 0 finishing.
       DEBUG: Writer 1 finishing.
       DEBUG: Writer 2 finishing.
       DEBUG: Writer 3 finishing.
       DEBUG: Writer 4 finishing.
       DEBUG: Reader 3 finishing.
       DEBUG: Reader 1 finishing.
       DEBUG: Reader 2 finishing.
       DEBUG: Reader 0 finishing.
       DEBUG: Reader 4 finishing.
        INFO: Backy complete.

As you can see, this will produce high amouts of output.


