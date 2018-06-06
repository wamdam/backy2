.. _administration-guide:

benji Administration guide
===========================

benji is an important tool when it's responsible for company-wide backups.
Backups, scrubs, restores and cleanups must run smoothly and need to be
monitored closely.

Also, as backy has two parts (metadata store and data backend), both have to
be checked regularily and be as highly-available as possible.

.. _administration-guide-meta-storage:

Secure the meta backend storage
-------------------------------

This section shows methods how to make the metadata store available even when
disasters happen.

benji export
~~~~~~~~~~~~~

The recommended way of keeping metadata along with your data is ``benji
export``:

.. command-output:: benji export --help

The *export* command will write metadata for a specific version into a (CSV)
file. This is roughly how this looks like::

    benji Version 2.2 metadata dump
    d91be794-2f21-11e7-b961-a44e314f9270,2017-05-02 10:26:48,test,,25600,104857600,1,0
    6ea578608ffuwQB2rhRMMevpJtVrNU7a,d91be794-2f21-11e7-b961-a44e314f9270,0,2017-05-02 12:26:51,04ca5d5da5270cf1e6a2ce09afc854a959eec7d59198b76436d3c40075b77f498d27d0891bdee01ccda017073390c150c01001b1c5e8289961c7a798a51a8964,4096,1
    d63675c78fMfgq9ULna3NwLBFvLNhy27,d91be794-2f21-11e7-b961-a44e314f9270,1,2017-05-02 12:26:51,bf80fb0bb63f1c79af7196ac8d5c0831c3fb9f1e532b2d190567a1351a689687b6892ae00d24a2db69d1a6f167670e2c34ddd81d4f453e934f7901df6f35f9f9,4096,1
    0cb2d82e64eg4eCMNixT79HpfEJnbZTB,d91be794-2f21-11e7-b961-a44e314f9270,2,2017-05-02 12:26:51,d619455cb43df5a7a5426ba1020ee47a79bd3ed0d0de977dbd99350569d4dff5647fcb9380a70e729d7891cc67a6f16a424a38ec1f1794097334091fb7a606ed,4096,1

.. NOTE:: benji tries very hard to support older versioned export data in
    newer versions.

After each backup you should export the generated version and store it together
with your backup data (that is in the same backup storage like NFS, S3, ...).

So even if your backup database server crashes, you'll still be able to reimport
all existing versions later.

.. NOTE:: **After** re-importing many versions, it is recommended to start a
    ``benji cleanup -f`` run as shown in section :ref:`full_cleanup`.

.. ATTENTION:: When you remove (``benji rm``) versions from the database and
    then call ``benji cleanup``, the blocks containing the backed up *data* will
    be removed. No ``benji import`` can bring them back, because benji's export
    format *only* contains metadata information.


benji import
~~~~~~~~~~~~~

In order to get an exported version back, you must import it again:

.. command-output:: benji import --help

Example::

    $ benji import myvm.20170421.benji

This will reimport with the same version UID as it had before (as this is stored
in the export file). benji will not allow to import a version UID which already
is in the database.


SQL high availability
~~~~~~~~~~~~~~~~~~~~~

An additional option against data loss is to mirror the sql database. All usual
mirroring techniques apply here. Please look into your database documentation.

.. CAUTION:: DBMS mirroring only helps when one server crashes or has a
    failure. It does not help against software-bug related data loss, human
    error and more. So benji export is the only reliable option for long-term
    data-safety.


High available data backend
---------------------------

Your data backend should be redundant in some way too. Examples are:

- RAID 1, 5, 6
- Redundant S3 compatible storage (riak cs, ceph object gateway, …)
- DRBD
- Some zfs mirroring should work too

If your data backend fails or has corruptions, at best corrupted restores will
be possible. benji does not store any redundant data neither can it restore
data from stored checksums.


Monitoring
----------

Tips & tricks
~~~~~~~~~~~~~

You should monitor exit codes of backy closely. Anything != 0 means: There was
a problem.

benji writes all output including possible tracebacks and command lines to
the logfile configured in backy.cfg (see :ref:`config_file`).
If anything goes wrong, you'll be able to visit this logfile and get
output, even if this benji call came from an automated script.

You should also monitor success of the backups. In addition to checking the
exit code, you can do this via ``benji ls`` and see if the column ``valid``
is 1. This will be 1 as soon as the backup has finished successfully.

You can also monitor progress of the backups either by looking at the mentioned
logfile or by checking your process-tree::

    $ ps axfu|grep "[b]acky2"
    …  \_ benji [Scrubbing Version 52da2130-2929-11e7-bde0-003048d74f6c (0.1%)]

To know which backup took how long and to see how many blocks/bytes have been
read and written, you can use the excellent ``benji stats`` command:

.. command-output:: benji stats --help

Example::

    $ benji stats -l3
        INFO: $ /home/dk/develop/benji/env/bin/benji stats -l3
    +---------------------+--------------------------------------+-------+------------+-------------+------------+-------------+---------------+----------------+-------------+--------------+--------------+---------------+--------------+
    |         date        |                 uid                  | name  | size bytes | size blocks | bytes read | blocks read | bytes written | blocks written | bytes dedup | blocks dedup | bytes sparse | blocks sparse | duration (s) |
    +---------------------+--------------------------------------+-------+------------+-------------+------------+-------------+---------------+----------------+-------------+--------------+--------------+---------------+--------------+
    | 2017-04-12 20:28:03 | 832dc202-1fbe-11e7-9f25-a44e314f9270 | small |   10485760 |        2560 |   10485760 |        2560 |      10485760 |           2560 |           0 |            0 |            0 |             0 |            7 |
    | 2017-04-12 20:28:26 | 90fbbeb6-1fbe-11e7-9f25-a44e314f9270 | small |   10485760 |        2560 |   10485760 |        2560 |             0 |              0 |    10485760 |         2560 |            0 |             0 |            7 |
    | 2017-05-02 10:27:48 | d91be794-2f21-11e7-b961-a44e314f9270 | test  |  104857600 |       25600 |  104857600 |       25600 |        323584 |             79 |   104534016 |        25521 |            0 |             0 |           60 |
    +---------------------+--------------------------------------+-------+------------+-------------+------------+-------------+---------------+----------------+-------------+--------------+--------------+---------------+--------------+
        INFO: Benji complete.


Machine output
~~~~~~~~~~~~~~

All commands in benji are available with machine compatible output too.
Columns will be pipe (``|``) separated.

Example::

    $ benji -m ls
    type|date|name|snapshot_name|size|size_bytes|uid|valid|protected|tags
    version|2017-04-18 18:05:04.174907|vm1|2017-04-19T11:12:13|25600|107374182400|c94299f2-2450-11e7-bde0-003048d74f6c|1|0|b_daily,b_monthly,b_weekly

    $ benji -m stats -l3
    type|date|uid|name|size bytes|size blocks|bytes read|blocks read|bytes written|blocks written|bytes dedup|blocks dedup|bytes sparse|blocks sparse|duration (s)
    statistics|2017-04-12 20:28:03|832dc202-1fbe-11e7-9f25-a44e314f9270|small|10485760|2560|10485760|2560|10485760|2560|0|0|0|0|7
    statistics|2017-04-12 20:28:26|90fbbeb6-1fbe-11e7-9f25-a44e314f9270|small|10485760|2560|10485760|2560|0|0|10485760|2560|0|0|7
    statistics|2017-05-02 10:27:48|d91be794-2f21-11e7-b961-a44e314f9270|test|104857600|25600|104857600|25600|323584|79|104534016|25521|0|0|60

With machine output, the log-level of benji is reduced to *WARNING*, no matter
what backy.cfg says.

.. HINT::
    Pipe separated content can be read easily with awk::

        awk -F '|' '{ print $3 }'

Debugging
~~~~~~~~~

In case anything goes wrong, you may use the DEBUG log-level. You can temporarily
enable this with the generic ``-v`` switch::

    $ benji -v ls
        INFO: $ /home/dk/develop/benji/env/bin/benji -v ls
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
        INFO: Benji complete.

As you can see, this will produce high amouts of output.


