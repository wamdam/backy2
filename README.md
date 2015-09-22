# backy

## An open source block based backup utility

backy is a **block based backup** for **virtual machines** with no external
dependencies. It's useful to backup any block based image with or without
snapshot features to any other location.

backy supports forward differential backups so that a full backup is only
necessary on the very first backup. All subsequent backups are forward
incremental, so that only the differences to the last backup are stored.

That way it requires only very little i/o on the backup volume.

In addition it can fully utilize all features which the underlying block device
supports. If hints are available, only relevant data sections are read.
Especially on ceph volumes, the first AND all subsequent backups only require
changed blocks to be stored.

On other volume types, if you can extract blocks used by the filesystem, also
only those blocks will be read.

backy makes heavy usage of rollsum files. Each ``chunk`` is checksummed and the
checksum is stored with the backup. Whenever a forward increment is created,
the source data is read and only compared to the stored checksum. That way,
the backup storage space's i/o is not used at all for determining which blocks
to read.

Also, scrubbing is available. Scrub can read the whole backup or any random
percentile and compare read backup data blocks against the stored checksums.
No bit loss anymore!

Scrub can also compare the backup against an existing image, i.g. a snapshot.
With this ``deep scrub`` also percentile checks are available.

When scrub finds invalid blocks, it marks them for being backed up on the
next run.

For each backup level, only changed *chunks* are stored so forward increments
are usually very small.

For restore, backy only writes existing blocks and sparses out the others.
That means, if you have a terabyte to restore, only the used blocks are
restored to an image, the rest is left "sparse".

So backy is, especially in combination with rich featured block storage, a
very fast and reliable backup and restore tool.

backy is designed to be as much unix-flavoured as possible. With defaults,
it mostly behaves like *cp* but with all features.


## Usage

```
usage: backy [-h] [-v] [-b BACKUPDIR] {backup,restore,scrub} ...

Backup and restore for block devices.

positional arguments:
  {backup,restore,scrub}
    backup              Perform a backup.
    restore             Restore a given backup with level to a given target.
    scrub               Scrub a given backup and check for consistency.

optional arguments:
  -h, --help            show this help message and exit
  -v, --verbose         verbose output (default: False)
  -b BACKUPDIR, --backupdir BACKUPDIR
```

### backup

```
usage: backy backup [-h] [-r RBD] source backupname

positional arguments:
  source             Source file
  backupname         Destination file. Will be a copy of source.

optional arguments:
  -h, --help         show this help message and exit
  -r RBD, --rbd RBD  Hints as rbd json format
```

### restore

```
usage: backy restore [-h] [-l LEVEL] backupname target

positional arguments:
  backupname
  target

optional arguments:
  -h, --help            show this help message and exit
  -l LEVEL, --level LEVEL
```

### scrub

```
usage: backy scrub [-h] [-l LEVEL] [-s SOURCE] [-p PERCENTILE] backupname

positional arguments:
  backupname

optional arguments:
  -h, --help            show this help message and exit
  -l LEVEL, --level LEVEL
  -s SOURCE, --source SOURCE
                        Source, optional. If given, check if source matches
                        backup in addition to checksum tests.
  -p PERCENTILE, --percentile PERCENTILE
                        Only check PERCENTILE percent of the blocks (value
                        0..100). Default: 100
```

## Examples

### Defaults

```
backy backup /dev/image /mnt/backups/image
backy restore /mnt/backups/image /dev/image
```

### Ceph

```
rbd --format json diff --from-snap someoldsnap vms/somevm@backup123 > mydiff
backy backup -r mydiff /vms/somevm@backup123 /mnt/backups/somevm
```

### lvm

tbd.


## Disclaimer

We don't take any responsibility if this software doesn't do what you think or
even if it doesn't do what is described here or anywhere else. It's up to you
to read the code yourself and decide if you want to use it.
We are using it and we wrote it for ourselves. If you're unsure, use it as
a template to develop your own backup software.

