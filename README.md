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

Invalid backups, due to data errors and otherwise broken backups (system crash)
are probably recognized (for bit rod we recommend scrubs) and you cannot backup
a diff on them.

On other volume types, if you can extract blocks used by the filesystem, also
only those blocks will be read.

backy makes heavy usage of rollsums. Each ``block`` is checksummed and the
checksum is stored with the backup. Whenever a forward increment is created,
the source data is read and only compared to the stored checksum. That way,
the backup storage space's i/o is not used at all for determining which blocks
to read.

Also, scrubbing is available. Scrub can read the whole backup or any random
percentile and compare read backup data blocks against the stored checksums.
No bit loss anymore!

Scrub can also compare the backup against an existing image, i.g. a snapshot.
With this ``deep scrub`` also percentile checks are available.

When scrub finds invalid blocks, it marks the blocks and all versions containing
this block as invalid (however, restores are still possible with this
deficiency).

For each backup version, only changed ``blocks`` are stored so forward increments
are usually very small.

For restore, backy is able to only write existing blocks and sparses out the
others. This is possible for new image files and on fresh volumes, e.g. on
ceph. This shouldn't be done on volumes that don't know about block sparses,
like classic partitions or classic lvm.

So backy is, especially in combination with rich featured block storage, a
very fast and reliable backup and restore tool.

backy is designed to be as much unix-flavoured as possible. With defaults,
it mostly behaves like *cp* but with all features.


## Usage

```
usage: backy [-h] [-v] [-b BACKUPDIR] {backup,restore,rm,scrub,cleanup,ls} ...

Backup and restore for block devices.

positional arguments:
  {backup,restore,rm,scrub,cleanup,ls}
    backup              Perform a backup.
    restore             Restore a given backup with level to a given target.
    rm                  Remove a given backup version. This will only remove
                        meta data and you will have to cleanup after this.
    scrub               Scrub a given backup and check for consistency.
    cleanup             Clean unreferenced blobs.
    ls                  List existing backups.

optional arguments:
  -h, --help            show this help message and exit
  -v, --verbose         verbose output (default: False)
  -b BACKUPDIR, --backupdir BACKUPDIR
```

### backup

```
usage: backy backup [-h] [-r RBD] [-f FROM_VERSION] source name

positional arguments:
  source                Source file
  name                  Backup name

optional arguments:
  -h, --help            show this help message and exit
  -r RBD, --rbd RBD     Hints as rbd json format
  -f FROM_VERSION, --from-version FROM_VERSION
                        Use this version-uid as base
```

### restore

```
usage: backy restore [-h] [-s] version_uid target

positional arguments:
  version_uid
  target

optional arguments:
  -h, --help    show this help message and exit
  -s, --sparse  Write restore file sparse (does not work with legacy devices)
```

### scrub

```
usage: backy scrub [-h] [-s SOURCE] [-p PERCENTILE] version_uid

positional arguments:
  version_uid

optional arguments:
  -h, --help            show this help message and exit
  -s SOURCE, --source SOURCE
                        Source, optional. If given, check if source matches
                        backup in addition to checksum tests.
  -p PERCENTILE, --percentile PERCENTILE
                        Only check PERCENTILE percent of the blocks (value
                        0..100). Default: 100
```

### rm
```
usage: backy rm [-h] version_uid

positional arguments:
  version_uid

optional arguments:
  -h, --help   show this help message and exit
```


### cleanup
```
usage: backy cleanup [-h]

optional arguments:
  -h, --help  show this help message and exit
```

## Examples

### Defaults

```
TBD
```

### Ceph

```
rbd --format json diff --from-snap someoldsnap vms/somevm@backup123 > mydiff
backy backup -r mydiff /vms/somevm@backup123 testbackup
```

### lvm

```
TBD
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

