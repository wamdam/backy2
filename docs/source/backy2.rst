.. include:: global.rst.inc

.. _backy2:

For Backy² Users
================

There is no direct migration path from Backy² to Benji. If you
want to migrate to Benji you'd have to start with a new installation.

The concepts and most of the command names and options stayed the same
so Benji should feel familiar to Backy² users. If you currently have
any scripts deployed together with Backy² they should require only
few changes to work with Benji. But if your scripts check for specific
exit codes from Backy² you'd have to adapt those because the have
all changed with Benji. Also if you used Backy²'s machine readable
output, there'd be changes required as Benji uses JSON instead of CSV.

Here's a list of the new features that differentiate Benji
from Backy²:

- Encryption support (AES-256, GCM mode)
- Compression support (zstandard)
- Automatic metadata export to data backend for backup purposes
- Support for Backblaze's B2 Cloud Storage as a *data backend*
- Variable block size for efficient support of LVM and Ceph in
  one installation of Benji
- More compact metadata storage, the metadata backend now uses
  significantly less disk space
- Database based locking to make it possible to have multiple instances
  on different hosts or in different containers
- New scrubbing mode based on metadata, object existence and length
- Randomly sampled bulk scrubbing of *versions*
- Simple yet flexible retention policy enforcement
- Migration from boto to boto3 with better compatibility for other
  S3 implementations (Google Storage for example)
- Rook integration via Kubernetes deployment based on a Helm chart
- blake2b with 32 byte digest as default hash function
- Optional read caching of blocks
- Configuration file format changed to YAML
- Machine output is JSON instead of CSV
- Import/export format is JSON instead of CSV

There also have been quite a few changes under the hood:

- Migration from classic threads and queues to concurrent.futures
- Use Ceph provided Python bindings
- Some refactoring and rewriting to share more code
- Update to database transaction handling
- Update to exit code handling, exit codes have changed
- SQLAlchemy based *metadata backend* is the only supported backend now
