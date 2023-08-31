.. include:: global.rst.inc

Backup
======

In this chapter you will learn all possibilities and options for backup.

.. command-output:: backy2 backup --help


Simple backup
-------------

This is how you can create a normal backup::

    $ backy2 backup source name

where source is a URI and name is the name for the backup, which may contain any
quotable character.

.. NOTE:: The name and all other identifiers are stored in SQL 'varchar'
    columns which are created by sqlalchemy's "String" type. Please refer to
    http://docs.sqlalchemy.org/en/latest/core/type_basics.html#sqlalchemy.types.String
    for reference.

The supported schemes for source are **file** and **rbd**. So these are
realistic examples::

    $ backy2 backup file:///var/lib/vms/database.img database
    $ backy2 backup rbd://poolname/database@snapshot1 database

If you need testdata for backup tests, there's also a ``null``-source which
creates demo data for you on demand::

    $ backy2 backup null://200GB testbackup

Supported sizes are::

  k or kB for kibibytes
  M or MB for mebibytes
  G or GB for gibibytes
  T or TB for tebibytes
  P or PB for pebibytes

.. NOTE:: The null:// source is only there to test performance of backy2 and
   the backup target and for testing RAM usage when sizes get larger. If you
   have other usecases, please let me know.

.. NOTE:: There's also a ``null`` backup target configuration available in
   backy.cfg if you want to also throw away backup data. This is also only
   there to test performance and RAM usage. With this and the null:// source
   you can backup petabytes of data from null to null just to test performance
   and RAM usage.


Stored version data
-------------------

An instance of a backup is called a *version*. A version contains these metadata
fields:

* **uid**: A UUID1 identifier for this version. This is created by backy2.
* **date**: The date and time of the backup. This is created by backy2.
* **name**: The name from the command line.
* **snapshot_name**: The snapshot name [-s] from the command line.
* **size**: The number of blocks (default: 4MB each) of the backed up image.
* **size_bytes**: The size in bytes of the image.
* **valid**: boolean (1/0) if the currently known state of the backup is valid.
  This is 0 while the backup for this version is running and will be set to 1
  as soon as the backup has finished and all writers have flushed their data.
  Scrubbing may set this to 0 if the backup is found invalid for any reason.
* **protected**: boolean (1/0): Indicates if the version may be deleted by *rm*.
* **tags**: A list of (string) tags for this version.
* **expire**: An optional expiration date for the version.

You can output this data with::

    $ backy2 ls
        INFO: $ /usr/bin/backy2 ls
    +---------------------+-------------------+---------------+------+------------+--------------------------------------+-------+-----------+----------------------------+------------+
    |         date        | name              | snapshot_name | size | size_bytes |                 uid                  | valid | protected | tags                       |   expire   |
    +---------------------+-------------------+---------------+------+------------+--------------------------------------+-------+-----------+----------------------------+------------+
    | 2017-04-17 11:54:07 | myfirsttestbackup |               |   10 |   41943040 | 8fd42f1a-2364-11e7-8594-00163e8c0370 |   1   |     0     | b_daily,b_monthly,b_weekly | 2020-12-30 |
    +---------------------+-------------------+---------------+------+------------+--------------------------------------+-------+-----------+----------------------------+------------+
        INFO: Backy complete.

.. HINT::
    You can filter the output with various parameters:

    .. command-output:: backy2 ls --help



.. _differential_backup:

Differential backup
-------------------

backy2 is able to only backup changed, non-sparse blocks. It can do this in two
different ways:

1. **It can read the whole image**, checksum each block and look the checksum up
   in the metadata backend. If it is found, only a reference to the existing
   block will be stored, thus there's no write action on the data backend.

2. **It can receive a hint file** ``[-r RBD, --rbd RBD Hints as rbd json format]``
   which contains a JSON formatted list of (offset, size) tuples (see
   :ref:`hints_file` for an example).
   Fortunately the format matches exactly to what ``rbd diff … --format=json``
   outputs.  In this case it will only read blocks hinted by the *hint file*,
   checksum each block and look the checksum up in the metadata backend. If it
   is still found (which may happen on file copies (rarelay) or when blocks are
   all \\0), only a reference to the existing block will be stored. Otherwise
   the block is written to the data backend.

.. NOTE:: backy2 does **forward-incremental backups**. So in contrast to
    backward-incremental backups, there will never be any need to create another
    full backup after a first full backup
    If you don't trust backy2 (which you always should with any software), you
    are encouraged to use ``backy2 scrub``, possibly with the ``[-s]``
    parameter to see if the backup matches the source.

.. HINT:: Even the first backup will be differential. Either because like in
    case 1, backy2 deduplicates blocks (in which case you may use tools like
    ``fstrim`` or ``dd`` to put a lot of \\0 to your empty space), or like in
    case 2 you can create a ``rbd diff`` without ``--from-snap`` which will
    create a list of used (=non-sparse) blocks (i.e. all non-used blocks will
    be skipped).

In any case, the backup source may differ in size. backy2 will then assume that
the size change has happened at the end of the volume, which is the case if you
resize partitions, logical volumes or rbd images.


Examples of differential backups
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

LVM (or any other diff unaware storage)
***************************************

Day 1 (initial backup)::

    $ lvcreate --size 1G --snapshot --name snap /dev/vg00/lvol1
    $ backy2 backup file:///dev/vg00/snap lvol1
    $ lvremove -y /dev/vg00/snap

Day 2..n (differential backups)::

    $ lvcreate --size 1G --snapshot --name snap /dev/vg00/lvol1
    $ backy2 backup file:///dev/vg00/snap lvol1
    $ lvremove -y /dev/vg00/snap

.. IMPORTANT:: With LVM snapshots, the snapshot increases in size as the origin
    volume changes. If the snapshot is 100% full, it is lost and invalid.
    It is important to monitor the snapshot usage with the ``lvs`` command
    to make sure the snapshot does not fill.
    The ``--size`` parameter defines the reserved space for changes during the
    snapshot existance.

    Also note that LVM does read-write-write for any overwritten block while a
    snapshot exists. This may hurt your performance.

ceph/rbd
********

With rbd it's possible to let ceph calculate the changes between two snapshots.
Since *ceph jewel* that is a very fast process, as only metadata has to be
compared (with the *fast-diff* feature enabled).


Manually
^^^^^^^^

In this example, we will backup an rbd image called ``vm1`` which is in the
pool ``pool``.

1. Create an initial backup::

    $ rbd snap create pool/vm1@backup1
    $ rbd diff --whole-object pool/vm1@backup1 --format=json > /tmp/vm1.diff
    $ backy2 backup -s backup1 -r /tmp/vm1.diff rbd://pool/vm1@backup1 vm1

2. Create a differential backup::

    $ rbd snap create pool/vm1@backup2
    $ rbd diff --whole-object pool/vm1@backup2 --from-snap backup1 --format=json > /tmp/vm1.diff

    # delete old snapshot
    $ rbd snap rm pool/vm1@backup1

    # get the uid of the version corrosponding to the old rbd snapshot. This
    # looks like "90fcbeb6-1fce-11c7-9c25-a44c314f9270". Copy it.
    $ backy2 ls vm1 -s backup1

    # and backup
    $ backy2 backup -s backup2 -r /tmp/vm1.diff -f 90fcbeb6-1fce-11c7-9c25-a44c314f9270 rbd://pool/vm1@backup2 vm1

Automation
^^^^^^^^^^

This is how you can automate forward differential backups including automatic
initial backups where necessary::

    function initial_backup {
        # call: initial_backup rbd vm1
        POOL="$1"
        VM="$2"

        SNAPNAME=$(date "+%Y-%m-%dT%H:%M:%S")  # 2017-04-19T11:33:23
        TEMPFILE=$(tempfile)

        echo "Performing initial backup of $POOL/$VM."

        rbd snap create "$POOL"/"$VM"@"$SNAPNAME"
        rbd diff --whole-object "$POOL"/"$VM"@"$SNAPNAME" --format=json > "$TEMPFILE"
        backy2 backup -s "$SNAPNAME" -r "$TEMPFILE" rbd://"$POOL"/"$VM"@"$SNAPNAME" $VM

        rm $TEMPFILE
    }

    function differential_backup {
        # call: differential_backup rbd vm1 old_rbd_snap old_backy2_version
        POOL="$1"
        VM="$2"
        LAST_RBD_SNAP="$3"
        BACKY_SNAP_VERSION_UID="$4"

        SNAPNAME=$(date "+%Y-%m-%dT%H:%M:%S")  # 2017-04-20T11:33:23
        TEMPFILE=$(tempfile)

        echo "Performing differential backup of $POOL/$VM from rbd snapshot $LAST_RBD_SNAP and backy2 version $BACKY_SNAP_VERSION_UID."

        rbd snap create "$POOL"/"$VM"@"$SNAPNAME"
        rbd diff --whole-object "$POOL"/"$VM"@"$SNAPNAME" --from-snap "$LAST_RBD_SNAP" --format=json > "$TEMPFILE"
        # delete old snapshot
        rbd snap rm "$POOL"/"$VM"@"$LAST_RBD_SNAP"
        # and backup
        backy2 backup -s "$SNAPNAME" -r "$TEMPFILE" -f "$BACKY_SNAP_VERSION_UID" rbd://"$POOL"/"$VM"@"$SNAPNAME" "$VM"
    }

    function backup {
        # call as backup rbd vm1
        POOL="$1"
        VM="$2"

        # find the latest snapshot name from rbd
        LAST_RBD_SNAP=$(rbd snap ls "$POOL"/"$VM"|tail -n +2|awk '{ print $2 }'|sort|tail -n1)
        if [ -z $LAST_RBD_SNAP ]; then
            echo "No previous snapshot found, reverting to initial backup."
            initial_backup "$POOL" "$VM"
        else
            # check if this snapshot exists in backy2
            BACKY_SNAP_VERSION_UID=$(backy2 -ms ls -s "$LAST_RBD_SNAP" "$VM"|awk -F '|' '{ print $6 }')
            if [ -z $BACKY_SNAP_VERSION_UID ]; then
                echo "Existing rbd snapshot not found in backy2, reverting to initial backup."
                initial_backup "$POOL" "$VM"
            else
                differential_backup "$POOL" "$VM" "$LAST_RBD_SNAP" "$BACKY_SNAP_VERSION_UID"
            fi
        fi
    }

    if [ -z $1 ] || [ -z $2 ]; then
            echo "Usage: $0 [pool] [image]"
            exit 1
    else
            rbd snap ls "$1"/"$2" > /dev/null 2>&1
            if [ "$?" != "0" ]; then
                    echo "Cannot find rbd image $1/$2."
                    exit 2
            fi
            backup "$1" "$2"
    fi

.. CAUTION:: This code is for demonstration purpose only. It should work however.

This is what it does:

* When called via ``command pool image``, it searches for the latest rbd
  snapshot. As rbd snapshots have no date assigned, it's the last one from
  ``rbd snap ls … | sort``.
* If none is found, an initial backup is performed.
* If there is a rbd snapshot, backy2 is asked if it has a *version* of this
  snapshot. If not, an initial_backup is performed.
* If backy2 has a *version* of this snapshot, a *diff* file is created via
  ``rbd diff --whole-object <new snapshot> --from-snap <old snapshot> --format=json``.
* backy2 then backs up according to changes found in this diff file.

So this script can be called each day (or even multiple times a day) and will
automatically keep only one snapshot and create forward-differential backups.

.. NOTE:: This alone will not be enough to be safe. You will have to perform
    additional scrubs. Please refer to section :ref:`scrubbing`.
    Also you will have to backup metadata exports along with your data, which
    will be handled in the next section.

Tag backups
-----------

backy2 provides predefined backup tags: b_daily, b_weekly, b_monthly
These tags are created automatically by comparing the dates of version with the
same name and only if you don't provide tags yourself (via the ``-t`` option
on backup).

If a specific tag should be used for a target backup revision, the backup
command provides the command line switch '-t' or '--tag':

    $ backy2 backup -t mytag rbd://cephstorage/test_vm test_vm

You can also use multiple tags for one revision, separated by comma:

    $ backy2 backup -t mytag,anothertag rbd://cephstorage/test_vm test_vm

Later on you can modify tags with the commands 'add-tag' and 'remove-tag':

    $ backy2 add-tag ea6faa64-6818-11e7-9a92-a0369f78d9c8 mytag
    $ backy2 remove-tag ea6faa64-6818-11e7-9a92-a0369f78d9c8 anothertag
    $ backy2 add-tag ea6faa64-6818-11e7-9a92-a0369f78d9c8 a,b,c,d
    $ backy2 remove-tag ea6faa64-6818-11e7-9a92-a0369f78d9c8 c,b


Expire backups
--------------

Backup expiration is used to mark backups as obsolete automatically at a given
date. The expiration can be set at backup time via '-e' or '--expire'::

    $ backy2 backup file:///tmp/test test -e 2020-01-24T04:00:00

You may also set or change the expiration date with the 'expire' command::

    $ backy2 expire 93e01e08-2af9-11ea-8e38-dc53608da00e 2020-02-01T04:00:00

Or you may remove the expiration date entirely by providing an empty string
as input for the 'expire' command::

    $ backy2 expire 93e01e08-2af9-11ea-8e38-dc53608da00e ''

The expire date is shown in the 'ls' command. In addition, 'ls' is able to
only show expired backups with its '-e' switch::

    $ backy2 ls -e

.. HINT::
    When scripting the backup, that's how you might add the expiration date::

        $ backy2 backup file:///tmp/test test -e `date +"%Y-%m-%d" -d "today + 7 days"`

Schedulers
----------

due
~~~

As you might have seen in the backy.cfg config file, backy has support for
individually defined schedulers. Here are some examples::

   [daily]
   interval: 1d
   keep: 8
   sla: 6h

   [weekly]
   interval: 7d
   keep: 5
   sla: 12h

   [monthly]
   interval: 30d
   keep: 3
   sla: 3d

Backy itself does not do anything itself just by these schedulers. You must
explicitly use them when calculating keep-times and so on.

That's where the ``backy2 due`` command kicks in:

.. command-output:: backy2 due --help

It checks for the given backup name (or for all if the name is skipped) together
with the information which schedulers to test for, if a new backup is due and
which expiration date should be set for it. If you don't pass schedulers,
backy2 will by default only use the ``daily`` scheduler::

    $ backy2 due
    +----------+------------+---------------------+---------------------+
    | name     | schedulers |     expire_date     |      due_since      |
    +----------+------------+---------------------+---------------------+
    | test     | daily      | 2020-11-19 21:39:20 | 1970-01-01 00:00:00 |
    | t        | daily      | 2020-11-19 21:39:20 | 2020-11-19 20:02:48 |
    +----------+------------+---------------------+---------------------+

The output is sorted with the oldest due_since on top.

Of course you can pass schedulers too::

   $ backy2 due -s hourly,daily test
       INFO: $ /root/backy2/env/bin/backy2 due -s hourly,daily test
   +------+--------------+---------------------+---------------------+
   | name | schedulers   |     expire_date     |      due_since      |
   +------+--------------+---------------------+---------------------+
   | test | hourly,daily | 2020-04-23 15:16:31 | 1970-01-01 00:00:00 |
   +------+--------------+---------------------+---------------------+
       INFO: Backy complete.

If you use the machine-output (``-m``) and short (``-s``) output options, you can
see that this information can easily be scripted::

   $ backy2 -ms due test
   test|daily|2020-04-23 15:13:56|1970-01-01 00:00:00

The calculation of the due date is::

  backup_time + sla_interval - sla_due

If you want to see how backy2 calculates the due date, pass ``-v``::

   $ backy2 -v due -s 10min t
   DEBUG: [backy2.logging] DUE:
        Last backup for t was at 2020-11-19 19:56:48.
        With the scheduler 10min, backup interval is 10m, SLA is 4m,
        so earliest due backup is at 2020-11-19 20:02:48.686034 and now is 2020-11-19 20:01:02.117573.


sla
~~~

If you want to check if for given schedulers there are not enough, too many, too old backups
or backups with too much time in between them, you can check this with the ``sla`` command:

.. command-output:: backy2 sla --help

Example::

   $ backy2 sla -s hourly,daily test
       INFO: $ /root/backy2/env/bin/backy2 sla -s hourly,daily test
   +------+-------------------------------------------------+
   | name | breach                                          |
   +------+-------------------------------------------------+
   | test | hourly: Too few backups. Found 0, should be 25. |
   | test | daily: Too few backups. Found 0, should be 6.   |
   +------+-------------------------------------------------+
       INFO: Backy complete.

.. NOTE:: If there's no sla breach, the table will be empty.


Export metadata
---------------

backy2 has now backed up all image data to a (hopefully) safe place. However,
the 4MB sized blocks are of no use without the corrosponding metadata. backy2
will need this information to get the blocks back in the correct order.

This information is stored in *metadata*. You must export the metadata and store
it to the backup storage. backy2 will not do this for you.

Otherwise, you'll lose all backups if you lose backy2's metadata storage which
resists on the backup server usually.

Just create an export file:

.. command-output:: backy2 export --help

Like this::

    $ backy2 export 52da2130-2929-11e7-bde0-003048d74f6c vm1.backy-metadata
    INFO: $ /usr/local/bin/backy2 export 52da2130-2929-11e7-bde0-003048d74f6c T
    INFO: Backy complete.

The created file is a simple CSV and can be re-imported to backy2::

    backy2 Version 2.2 metadata dump
    52da2130-2929-11e7-bde0-003048d74f6c,2017-04-24 22:05:04,zimbra.trusted@backup_20170424214643,,214000,897581056000,1,0
    38fdb171ccdm34m59W8wMCDiArpTRTsF,52da2130-2929-11e7-bde0-003048d74f6c,0,2017-04-24 22:11:14,d85694f3969a59aece4ab3758f25f3bf8f2e4223b7b69b701843f0292b9c857eb4f5d157d365f194c093a7014dec419dc54c868b6ed7fde8f572583b4b75520b,4194304,1
    3cf9e33358aQdAqmX7LtWNFVAjsZTw5S,52da2130-2929-11e7-bde0-003048d74f6c,1,2017-04-24 22:11:14,a1e9bc0b8aa9579360b9c71685de3e54eb70b8be2a915676b9dd100d5bbd40a91c71b1920a971c291d8643b334e88077592a12d41843bab138257c6cb2b01bfd,4194304,1
    …

However, backy2 will ignore your request if the version uid is already in the database. ::

    $ backy2 import vm1.backy-metadata
    INFO: $ /usr/local/bin/backy2 import vm1.backy-metadata
    ERROR: 'Version 52da2130-2929-11e7-bde0-003048d74f6c already exists and cannot be imported.'

Otherwise the version will show up after importing it when looking at ``backy2 ls``.

.. HINT::
    backy2 has compatibility layers for older backups, so imports from older
    metadata versions should work without problems.


Features
--------

Machine output
~~~~~~~~~~~~~~

All commands in backy2 are available with machine compatible output too.
Columns will be pipe (``|``) separated.

Example::

    $ backy2 -m ls
    type|date|name|snapshot_name|size|size_bytes|uid|valid|protected|tags
    version|2017-04-18 18:05:04.174907|vm1|2017-04-19T11:12:13|25600|107374182400|c94299f2-2450-11e7-bde0-003048d74f6c|1|0|b_daily,b_monthly,b_weekly

.. HINT::
    Pipe separated content can be read easily with awk::

        awk -F '|' '{ print $3 }'

.. HINT::
    For simplicity you can skip the header with the ``-s`` switch::

        $ backy2 -ms ls




Progress in process tree
~~~~~~~~~~~~~~~~~~~~~~~~

When automating backup, scrub and restore jobs, it's hard to keep track of what's
going on when looking only at log files.

For this, backy2 updates its progress in the process tree. So in order to watch
backy2's progress, just look at ::

    $ ps axfu|grep "[b]acky2"
    …  \_ backy2 [Scrubbing test (9054672e-7e3e-11ea-a694-003048d74f6c) Read Queue [          ] Write Queue [          ] (2.0% 2.4MB/s ETA 83s)]
    …  \_ backy2 [Backing up (2/2: Data) rbd://vms/test@backy2_20200415111550 Read Queue [==========] Write Queue [==========] (11.5% 93.0MB/sØ ETA 59h1m) ]

.. _hints_file:

The *hints file*
----------------

Example of a hints-file::

    [{"offset":0,"length":4194304,"exists":"true"},{"offset":4194304,"length":4194304,"exists":"true"},{"offset":952107008,"length":4194304,"exists":"true"}]

.. NOTE:: The length may vary, however it's nicely aligned to 4MB when using
    ``rbd diff --whole-object``. As backy2 per default also uses 4MB blocks,
    backy will not have to recalculate which 4MB blocks are affected by more
    and smaller offset+length tuples (not that that'd take very long).

Backup continuation
-------------------

If you backup target is unreliable and your backups take a long time it may
happen that backy2 stops working because the backup target is down, unreachable
or throws errors (actually you may also just kill the backy2 process by pressing
ctrl+c or killing the process).

In this case backy2 will *not* mark the version as valid.

You can of course just start the backup again - even from the same snapshot. That
will create a new version and backup from the start.

However if your backup takes longer than your backup target can usually be
reliable (for whatever reason, might also be networking related), you may use
the ``--continue-version`` (or ``-c``) option for ``backy2 backup``.

You must ensure yourself that all other parameters are identical when continuing a
backup. Otherwise you'll just backup garbage.

Here's an example for backing up from a snapshot::

   $ rbd snap create pool/vm1@backup1
   $ rbd diff --whole-object pool/vm1@backup1 --format=json > /tmp/vm1.diff
   $ backy2 backup -s backup1 -r /tmp/vm1.diff rbd://pool/vm1@backup1 vm1

Now if the backup stops somehow you will get an error message and the backup will
not be valid. Example::

   $ backy2 ls
       INFO: $ backy2 ls
   +---------------------+-------------+---------------+--------+---------------+--------------------------------------+-------+-----------+…
   |         date        | name        | snapshot_name |   size |    size_bytes |                 uid                  | valid | protected |…
   +---------------------+-------------+---------------+--------+---------------+--------------------------------------+-------+-----------+…
   | 2020-04-16 06:13:23 | test        | backup1       |     33 |        133809 | af6478e3-2af2-11ea-8e38-dc53608da00e |   0   |     0     |…
   +---------------------+-------------+---------------+--------+---------------+--------------------------------------+-------+-----------+…
       INFO: Backy complete.

Now you can continue this backup if the snapshot and the diff file still exist
if you pass backy2 the version uid for the backup to continue from::

   $ backy2 backup -s backup1 -r /tmp/vm1.diff -c af6478e3-2af2-11ea-8e38-dc53608da00e rbd://pool/vm1@backup1 vm1

Backy will only check if the backup source has the same size as saved in the
version (as a little bit of a sanity check) and if the version is marked as
invalid::

   $ backy2 backup null://1GB test1gb -c 30d53cea-7ff8-11ea-9466-8931a4889813
       INFO: $ backy2 backup null://1GB test1gb -c 30d53cea-7ff8-11ea-9466-8931a4889813
      ERROR: Unexpected exception
      ERROR: You cannot continue a valid version.
   Traceback (most recent call last):
     File "/home/dk/develop/backy2/src/backy2/scripts/backy.py", line 749, in main
       func(**func_args)
     File "/home/dk/develop/backy2/src/backy2/scripts/backy.py", line 95, in backup
       version_uid = backy.backup(name, snapshot_name, source, hints, from_version, tags, expire_date, continue_version)
     File "/home/dk/develop/backy2/src/backy2/backy.py", line 646, in backup
       raise ValueError('You cannot continue a valid version.')
   ValueError: You cannot continue a valid version.
       INFO: Backy failed.


