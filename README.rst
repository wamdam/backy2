Benji Backup
############

Benji Backup is a block based deduplicating  backup software. It builds on the
excellent foundations and concepts of `backy² <http://backy2.com/>`_ by Daniel Kraft.
Many thanks go to him for making his work public and releasing backy² as
open-source software!

The primary use cases for Benji are:

* Fast and resource-efficient backup of Ceph RBD images to object or file storage
* Backup of LVM volumes (e.g. from servers or personal computers) to external hard
  drives or the cloud

Benji features a Docker image and Helm chart for integration with
`Kubernetes <https://kubernetes.io/>`_ and  `Rook <https://rook.io/>`_. This makes it
easy to setup a backup solution for your persistent volumes.

Status
------

Benji is currently somewhere between alpha and beta quality. It passes all included
tests with the exception of NBD. So NBD is probably broken right now and needs
some minor work. The documentation is mostly up-to-date but the web site is not.
Installation with ``pip install`` should work fine.

Benji requires **Python 3.6.5 or newer** because older Python versions
have some shortcomings in the concurrent.futures implementation which lead to an
excessive memory usage.

Main features
-------------

**Small backups**
    Benji deduplicates while reading from the block device and only writes
    blocks once if they have the same checksum. Deduplication takes into
    account all historic data present in the backup storage target and so
    spans all backups and all backup sources. This can make deduplication
    more effective if images are clones of a common ancestor.

**Fast backups**
    With the help of Ceph's ``rbd diff``, Benji will only read the blocks
    that have changed since the last backup. Even when this information
    is not available (like with LVM) Benji will of course still only backup
    changed blocks.

**Fast restores**
    With supporting block storage (like Ceph's RBD), a sparse restore is
    possible. This means, sparse blocks (i.e. blocks which are holes or are
    all zeros) will be skipped on restore.

**File-based restores**
    Benji brings its own NBD (network block device) server which makes backup
    images directly mountable and enables file-based restores - even via the
    network on another machine.

    These mounts are read/write (unless you specify ``-r``) and writing to them
    creates a copy-on-write backup version (**i.e. the original version is not**
    **modified**).

**Small bandwidth requirements**
    As only changed blocks are written to the backup target, a small connection
    is sufficient even for larger backups. Even with newly created block devices
    the traffic to the backup target is small, because these block devices usually
    contain mostly zeros and are deduplicated before reaching the target storage.

    In addition to this Benji supports fast state-of-the-art compression based on
    `zstandard <https://github.com/facebook/zstd>`_. This will further reduce the
    required bandwidth and also reduce the storage space requirements.

**Support for a variety of backup storage targets**
    Benji supports AWS S3 as a data backend but also has options to enable
    compatibility with other S3 implementations like Google Storage, Ceph
    RADOS Gateway or `Minio <https://www.minio.io/>`_.

    Benji also supports `Backblaze's <https://www.backblaze.com/>`_ B2 Cloud
    Storage which opens up a very cost effective way to keep your backups.

    Last but not least Benji can also use any file based storage including
    external hard drives and NFS based storage solutions.

**Confidentiality**
    Benji supports AES-256 in GCM mode to encrypt all your data on the backup
    storage target. By using envelope encryption every block is encrypted
    with its own unique random key which makes plaintext attacks even more
    difficult.

**Integrity protection**
    Every backed up block keeps a checksum with it. When Benji scrubs the backup,
    it reads the block from the backup target storage, calculates its
    checksum and compares it to the stored checksum. If the checksum differs,
    it's most likely that there was an error while storing or reading
    the block, or because of bit rot on the backup target storage.

    Benji also supports a faster light-weight scrubbing mode which only checks
    the meta data consistency and object existence on the target storage.

    If a scrubbing failure occurs, the defective block and the backups it belongs
    to are marked as 'invalid' and the block will be re-read for the next backup
    version even if ``rbd diff`` indicates that it hasn't changed.

    Scrubbing can also take a percentage value of how many blocks of the backup
    it should scrub. So you can statistically scrub 16% each day and have a
    full scrub each week (16*7 > 100).

    .. NOTE:: Even invalid backups can be restored!

**Concurrency: Backup while scrubbing while restoring**
    As Benji is a long-running process, you don't want to wait until something has
    finished of course. While there are a few places in Benji where
    a global lock will be held most operations can run in parallel. So you
    can scrub, backup and restore at the same time and multiple times each.

    Benji even supports distributed operation where multiple instances run on
    different hosts or in different containers at the same time.

**Cache friendly**
    While reading large pieces of data on Linux, often buffers and caches get filled
    up with data (which in case of backups is essentially only needed once).
    Benji instructs Linux and Ceph to immediately forget the data once it's processed.

**Simplicity: As simple as cp, but as clever as a backup solution needs to be**
    With a very small set of commands, good ``--help`` and intuitive usage,
    Benji feels mostly like ``cp``. And that's intentional, because we think,
    a restore must be fool-proof and succeed even if you're woken up at 3am.

**Prevents you from doing something stupid**
    By providing a configuration value for how old backups need to be in order to
    be able to delete them, you can't accidentally delete very young backups. An
    exception to this is the enforcement of retention policies which will also
    delete young backups if configured.

    With ``benji protect`` you can protect versions from being deleted.
    This is very important when you need to restore a version which according to the
    retention policy may be deleted soon. During restore a lock will also prevent
    deletion, however, by protecting it, it cannot be deleted until you decide
    that it's not needed anymore.

    Also, you'll need to use ``--force`` to overwrite existing files or volumes.

**Free and Open Source Software**
    Anyone can review the source code and audit security and functionality.
    Benji is licensed under the LGPLv3 license. Please see the documentation
    for a full list of licenses.




