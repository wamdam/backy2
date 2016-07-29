# backy

## An open source block based backup utility with sparse features

backy is a **block based backup** for **virtual machines**, runs in linux and is
LGPL v3 licensed.

backy backs up from

* rbd images or snapshots (ceph)
* lvm images or snapshots
* partitions
* virtual machine image files
* any block-device file

backy stores backups to

* s3 compatible storage
* any filesystem (nfs, ext2/3/4, zfs, ...)

Readers and writers are written as plugins and are configurable, so extension
is *very* easy.

backy is useful to backup any block based image with or without snapshot features
to any other location, that is a filesystem (NFS, ...) or any S3 compatible
storage.

backy supports forward differential backups so that a full backup is only
necessary on the very first backup (and with ceph, not even that). All
subsequent backups are forward incremental, so that only the differences to the
last backup are stored.

That way it requires only very little i/o on the backup volume.

In addition it can fully utilize all features which the underlying block device
supports. If hints are available (e.g. rbd diff), only relevant data sections
are read.
Especially on ceph volumes, the first AND all subsequent backups only require
changed blocks to be read and stored.

Invalid backups, due to data errors and otherwise broken backups (system crash)
are recognized (for bitrot we recommend `backy scrub`) and backy prevents you
from backing up from them as a base (i.e. you must do a fresh full backup).

Backy also does deduplication during backup. This means, that equal blocks,
which are per default 4MB in size, are only stored once on the backup medium.

For protection against bitrot or write/read errors, backy supports scrubs.
We recommend to run scrubs on a regular basis.

Scrubs support a percentile parameter (-p). When percentile is given, only
the given part (percentile / 100) of the backup is read and compared against
the stored checksums.

Scrub can also compare the backup against an existing image, e.g. a snapshot.
With this ``deep scrub`` percentile checks are also available. Deep scrubs find
especially when the source has changed.

When scrub finds invalid blocks, it marks the blocks and all versions containing
this block as invalid (however, restores are still possible with this
deficiency).

For restore, backy is able to only write existing blocks and sparses out the
others. This is possible for new image files and on fresh volumes, e.g. on
ceph. This shouldn't be done on volumes that don't know about block sparses,
like classic partitions or classic lvm.

For example, with ceph it's possible to have sparse images by regularily calling
fstrim in the VMs.

In case you don't have a ceph-based storage, backy behaves correctly on other
images too, for example LVM, image files or partitions. However, the image
to be backed up shouldn't have any writes during backup. So snapshot
functionality is necessary if you can't afford downtime during backups.

So backy is, especially in combination with rich featured block storage, a
very fast and reliable backup and restore tool.

backy is designed to be as much unix-flavoured as possible. With defaults,
it mostly behaves like *cp* but with all features.

Other features:

* Restore during Backup: You can restore any valid version even while backups
  are running.
* RAM management: Backy runs on terabyte volumes with <350MB RAM typically
  (+SQL DB).
* RAM management: Backy removes all data it has read from the kernel
  buffers/caches so that influence on cache performance is minimal.
* Restore of invalid blocks/versions is possible for partial restores, warnings
  are logged.
* Metadata from SQL can (and should) be exported and imported along with the
  backup
* Any backup version can be mapped with nbd and so directly mounted
* Mapped backup versions can be read-only or read-write
* Read-write mapped backup versions create a new backup version on writes (copy
  on write). So you can test if your backed up e.g. database server starts
  smoothly without restoring it first.


## Installation

  ```
   git clone https://github.com/wamdam/backy2.git
   cd backy2
   make
   sudo make install
  ```

  Tested under ubuntu.


## Usage

### Backup

```
# backy2 backup -h

usage: backy2 backup [-h] [-r RBD] [-f FROM_VERSION] source name

positional arguments:
  source                Source (url-like, e.g. file:///dev/sda or
                        rbd://pool/imagename@snapshot)
  name                  Backup name

optional arguments:
  -h, --help            show this help message and exit
  -r RBD, --rbd RBD     Hints as rbd json format
  -f FROM_VERSION, --from-version FROM_VERSION
                        Use this version-uid as base
```

### Initial backup

#### ceph

With ceph, you can use a rbd diff even on the first snapshot in order to leave
out sparse blocks (hint: do a fstrim first!).

```
rbd snap create vms/vm1@backup1
rbd diff vms/vm1@backup1 --format=json > /tmp/vm1@backup1.diff
backy2 backup rbd://vms/vm1@backup1 vm1@backup1

    INFO: $ /home/dk/develop/backy2/env/bin/backy2 backup rbd://rbd/test@snap1 rbdtest2
    INFO: New version: e3106ffa-5564-11e6-9bd5-a44e314f9270
    INFO: Backy complete.
```

This creates a snapshot in pool `vms` from the image `vm1` called vm1@backup1.
Then it creates a json-formatted diff 'from the beginning', that means, the
diff defines all used areas.

Then the snapshot is backed up by backy with the rbd diff file as hints to
backup the block device into a backup called `vm1@backup1`.


#### lvm

```
lvm TODO
Mostly about creating a snapshot and calling sth like

backy2 backup file://dev/mapper/something name
```


### Incremental backup

#### ceph

An incremental backup with ceph is mostly equal to the initial one, except that
we will tell backy2 and ceph to base the new backup on an old one.

For ceph, the old snapshot name is ok, for backy2 we need the backup uid::

```
# backy2 ls
    INFO: $ backy2 ls
+----------------------------+---------------------+-------+--------------+--------------------------------------+---------------+
|            date            |        name         |  size |  size_bytes  |                 uid                  | version valid |
+----------------------------+---------------------+-------+--------------+--------------------------------------+---------------+
| 2015-10-30 16:28:57.262395 |     vm1@backup1     | 62500 | 262144000000 | 90811ff0-83f5-11e5-ad76-003148d6aacc |       1       |
+----------------------------+---------------------+-------+--------------+--------------------------------------+---------------+
    INFO: Backy complete.
```

The backup uid can also be grabbed with the machine output::

```
# backy2 -m ls | grep 90811ff0-83f5-11e5-ad76-003148d6aacc
version 2015-10-30 16:28:57.262395 vm1@backup1 62500 262144000000 90811ff0-83f5-11e5-ad76-003148d6aacc 1
```

Then we create an incremental backup::

```
rbd snap create vms/vm1@backup2
rbd diff vms/vm1@backup2 --from-snap vms/vm1@backup1 --format=json > /tmp/vm1@backup2.diff
rbd map vms/vm1@backup2
backy2 backup -r /tmp/vm1@backup2.diff -f 90811ff0-83f5-11e5-ad76-003148d6aacc rbd://vms/vm1@backup2 vm1@backup2
```

Notice the -f (--from) for backy. This tells backy2 to use all the metadata
(blocks, checksums, ...) from the old backup and only overwrite the parts given
in the hints-file (-r).
You must ensure that the from-backup (-f) matches the snapshot from which the
diff was created (--from-snap). Otherwise the wrong data sections will be stored
and the backup is invalid without backy2 being able to notice this.


### Metadata

The metadata from backy2 is saved in an sql database that can be configured in
your backy.cfg (usually /etc/backy.cfg). Default is sqlite, but we recommend
PostgreSQL.

In any case, the metadata database is required to be intact when restoring data.
You may use several mirrors for the database (e.g. MySQL, PostgreSQL) or, or even
in addition, you may want to store the metadata for each backup along with the
backup data.

To do this, export it:

```
# backy2 ls
    INFO: $ backy2 ls
+----------------------------+---------------------+-------+--------------+--------------------------------------+---------------+
|            date            |        name         |  size |  size_bytes  |                 uid                  | version valid |
+----------------------------+---------------------+-------+--------------+--------------------------------------+---------------+
| 2015-10-30 16:28:57.262395 |     vm1@backup1     | 62500 | 262144000000 | 90811ff0-83f5-11e5-ad76-003148d6aacc |       1       |
+----------------------------+---------------------+-------+--------------+--------------------------------------+---------------+
    INFO: Backy complete.

# backy2 export 90811ff0-83f5-11e5-ad76-003148d6aacc /var/backup/vm1@backup1.metadata
    INFO: $ backy2 export 90811ff0-83f5-11e5-ad76-003148d6aacc /var/backup/vm1@backup1.metadata
    INFO: Backy complete.
```

The metadata file contains anything that backy2 requires to be able to restore
from the data directory (of course, actual data blocks are also required).

Let's try it out (you don't need to do this on live backup systems):

```
# backy2 rm 90811ff0-83f5-11e5-ad76-003148d6aacc
    INFO: $ backy2 rm 90811ff0-83f5-11e5-ad76-003148d6aacc
    INFO: Backy complete.

# backy2 ls
    INFO: $ backy2 ls
+----------------------------+---------------------+-------+--------------+--------------------------------------+---------------+
|            date            |        name         |  size |  size_bytes  |                 uid                  | version valid |
+----------------------------+---------------------+-------+--------------+--------------------------------------+---------------+
+----------------------------+---------------------+-------+--------------+--------------------------------------+---------------+
    INFO: Backy complete.

# backy2 import /var/backup/vm1@backup1.metadata
    INFO: $ backy2 import /var/backup/vm1@backup1.metadata
    INFO: Backy complete.

# backy2 ls
    INFO: $ backy2 ls
+----------------------------+---------------------+-------+--------------+--------------------------------------+---------------+
|            date            |        name         |  size |  size_bytes  |                 uid                  | version valid |
+----------------------------+---------------------+-------+--------------+--------------------------------------+---------------+
| 2015-10-30 16:28:57.262395 |     vm1@backup1     | 62500 | 262144000000 | 90811ff0-83f5-11e5-ad76-003148d6aacc |       1       |
+----------------------------+---------------------+-------+--------------+--------------------------------------+---------------+
    INFO: Backy complete.
```

As you can see, original dates, name, sizes and uids are restored, as well as
information about the validity of the backup.



### Restore

```
# backy2 ls
    INFO: $ env/bin/backy2 ls
+----------------------------+---------------------+-------+--------------+--------------------------------------+---------------+
|            date            |        name         |  size |  size_bytes  |                 uid                  | version valid |
+----------------------------+---------------------+-------+--------------+--------------------------------------+---------------+
| 2015-10-30 16:28:57.262395 |     vm1@backup1     | 62500 | 262144000000 | 90811ff0-83f5-11e5-ad76-003148d6aacc |       1       |
+----------------------------+---------------------+-------+--------------+--------------------------------------+---------------+
    INFO: Backy complete.

# backy2 restore 3117eecc-8369-11e5-ad76-003048d6aadd /tmp/restore.img
```

That would restore the given uid into a file `tmp/restore.img`.
However, restore has an additional option '-s'. If your target supports sparse
writes (which are *fresh* ceph volumes and image files - actually they have to
fill the sparse blocks with \0), you can use -s to skip over sparse blocks and
thus speed up the restore process.

If you use -s with an existing block device (partition, old lvm volume, ...),
blocks that should be \0 only will have random old data in them. This might
be ok, if nothing of your system depends on having \0 there, but I wouldn't bet
on this.


### Scrub

For production systems, scrubbing is highly recommended in order to prevent
hidden bitrot, filesystem errors on the backup system and more.

backy2 can scrub against its own checksums or even against original snapshots.

#### Checksum based scrub

```
# backy2 scrub 90811ff0-83f5-11e5-ad76-003148d6aacc

TODO Output
```

#### deep scrub

```
rbd map vms/vm1@backup1
backy2 scrub -s /dev/rbd/vms/vm1@backup1 90811ff0-83f5-11e5-ad76-003148d6aacc
rbd unmap vms/vm1@backup1

TODO Output
```

### Remove old backups

TODO

```
backy2 rm 90811ff0-83f5-11e5-ad76-003148d6aacc
# wait 1 hour, because cleanup will only run old delete jobs
backy2 cleanup
```


## Disclaimer

This software is in a very early stage and we are currently testing it. You
should not rely on it to work probably. Invalid backups and data loss are
possible and the software is currently mostly untested.

We don't take any responsibility if this software doesn't do what you think or
even if it doesn't do what is described here or anywhere else. It's up to you
to read the code yourself and decide if you want to use it.

We are using it and we wrote it for ourselves. If you're unsure, use it as
a template to develop your own backup software.

