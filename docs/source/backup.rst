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

You can output this data with::

    $ backy2 ls
        INFO: $ /usr/bin/backy2 ls
    +---------------------+-------------------+---------------+------+------------+--------------------------------------+-------+-----------+----------------------------+
    |         date        | name              | snapshot_name | size | size_bytes |                 uid                  | valid | protected | tags                       |
    +---------------------+-------------------+---------------+------+------------+--------------------------------------+-------+-----------+----------------------------+
    | 2017-04-17 11:54:07 | myfirsttestbackup |               |   10 |   41943040 | 8fd42f1a-2364-11e7-8594-00163e8c0370 |   1   |     0     | b_daily,b_monthly,b_weekly |
    +---------------------+-------------------+---------------+------+------------+--------------------------------------+-------+-----------+----------------------------+
        INFO: Backy complete.


Differential backup
-------------------

backy2 is (only) able to backup changed blocks. It can do this in two different
ways:

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

.. TODO: TRY THIS AND FINALIZE THIS

Day 1 (initial backup)::

    $ export SNAPNAME=$(date "+%Y-%m-%d")  # 2017-04-19
    $ rbd snap create rbd/vm1@"$SNAPNAME"
    $ rbd diff --whole-object rbd/vm1@"$SNAPNAME" --format=json > /tmp/vm1.diff
    $ backy2 backup -s "$SNAPNAME" -r /tmp/vm1.diff rbd://rbd/vm1@"$SNAPNAME" backup_vm1

    $ # DO NOT DELETE THE SNAPSHOT

Day 2..n (differential backups)::

    $ export SNAPNAME=$(date "+%Y-%m-%d")  # 2017-04-20
    $ # find the latest common snapshot name
    $ export EXISTING_SNAPS_RBD=$(rbd snap ls vms/vm1|tail -n +2|awk '{ print $2 }'|sort)
    $ export EXISTING_SNAPS_BACKY2=$(backy2 -m ls|grep -e '^version|[^\|]*|vm1'|awk -F '|' '{ print $4 }'|grep -v -e '^$')
    $ export BEST_COMMON_SNAP=$(comm -12 <(echo "$EXISTING_SNAPS_RBD") <($EXISTING_SNAPS_BACKY2))
    $ export OLDSNAPNAME=$(rbd snap ls rbd/vm1|grep backup_|awk '{ print $2 }')
    $ # backy2 ls orders output by name, date.
    $ rbd snap create rbd/vm1@"$SNAPNAME"
    $ rbd diff --whole-object rbd/vm1@"$SNAPNAME" --from-snap $OLDSNAPNAME --format=json > /tmp/vm1.diff
    $ rbd snap rm rbd/vm1@"$OLDSNAPNAME"
    $ backy2 backup -s "$SNAPNAME" -r /tmp/vm1.diff -f $OLDVERSION rbd://rbd/vm1@"$SNAPNAME" vm1

.. CAUTION:: This example is for demonstration purpose only and not at all as
    fool-proof as it should be for backups. Please script your own logic of how
    to find yesterday's snapshots et.al.

This example is a simplified version of what you can script for your backups.
This is what it does:

* On day 1, it creates a snapshot from the vm *vm1* in pool *rbd* called
  *2017-04-19*.
* After that, the command ``rbd diff`` creates a list of used blocks of this
  image. If you want to keep backup time short even for this very first backup,
  you may call ``fstrim`` (if your storage driver supports it) or
  ``dd if=/dev/zero of=T bs=1M; rm T`` inside the VM in order to zero a large
  part of the storage. With this, backy2 (or rbd) can deduplicate empty parts.
* Then backy2 backups the image under the name *backup_vm1* with the snapshot
  name *2017-04-19*. The latter is to be able to find the old snapshot name again
  tomorrow (i.e. the snapshot name stored in backy2 matches the snapshot name
  in ceph).


TODO

- machine output
- Export metadata
- percentage in process output


.. _hints_file:

The *hints file*
----------------

Example of a hints-file::

    [{"offset":0,"length":4194304,"exists":"true"},
    {"offset":4194304,"length":4194304,"exists":"true"},
    {"offset":8388608,"length":4194304,"exists":"true"},
    {"offset":12582912,"length":4194304,"exists":"true"},
    {"offset":16777216,"length":4194304,"exists":"true"},
    {"offset":20971520,"length":4194304,"exists":"true"},
    {"offset":25165824,"length":4194304,"exists":"true"},
    {"offset":952107008,"length":4194304,"exists":"true"}

.. NOTE:: The length may vary, however it's nicely aligned to 4MB when using
    ``rbd diff --whole-object``. As backy2 per default also uses 4MB blocks,
    backy will not have to recalculate which 4MB blocks are affected by more
    and smaller offset+length tuples (not that that'd take very long).
