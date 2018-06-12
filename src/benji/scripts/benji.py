#!/usr/bin/env python
# -*- encoding: utf-8 -*-

import argparse
import fileinput
import logging
import os
import random
import sys

import pkg_resources
from prettytable import PrettyTable

import benji.exception
from benji.benji import Benji, BenjiStore
from benji.config import Config
from benji.logging import logger, init_logging
from benji.metadata import Version, VersionUid
from benji.nbdserver import NbdServer
from benji.utils import hints_from_rbd_diff, PrettyPrint

__version__ = pkg_resources.get_distribution('benji').version


class Commands:
    """Proxy between CLI calls and actual backup code."""

    def __init__(self, machine_output, config):
        self.machine_output = machine_output
        self.config = config

    def backup(self, name, snapshot_name, source, rbd, base_version_uid, block_size=None, tags=None):
        base_version_uid = VersionUid.create_from_readables(base_version_uid)
        benji_obj = None
        try:
            benji_obj = Benji(self.config, block_size=block_size)
            hints = None
            if rbd:
                data = ''.join([line for line in fileinput.input(rbd).readline()])
                hints = hints_from_rbd_diff(data)
            backup_version_uid = benji_obj.backup(name, snapshot_name, source, hints, base_version_uid, tags)
            if self.machine_output:
                benji_obj.export_any(
                    'versions', [benji_obj.ls(version_uid=backup_version_uid)],
                    sys.stdout,
                    ignore_relationships=[((Version,), ('blocks',))])
        finally:
            if benji_obj:
                benji_obj.close()

    def restore(self, version_uid, target, sparse, force, metadata_backend_less=False):
        version_uid = VersionUid.create_from_readables(version_uid)
        benji_obj = None
        try:
            benji_obj = Benji(self.config, in_memory=metadata_backend_less)
            if metadata_backend_less:
                benji_obj.import_from_backend([version_uid])
            benji_obj.restore(version_uid, target, sparse, force)
        finally:
            if benji_obj:
                benji_obj.close()

    def protect(self, version_uids):
        version_uids = VersionUid.create_from_readables(version_uids)
        benji_obj = None
        try:
            benji_obj = Benji(self.config)
            for version_uid in version_uids:
                try:
                    benji_obj.protect(version_uid)
                except benji.exception.NoChange:
                    logger.warning('Version {} already was protected.'.format(version_uid))
        finally:
            if benji_obj:
                benji_obj.close()

    def unprotect(self, version_uids):
        version_uids = VersionUid.create_from_readables(version_uids)
        benji_obj = None
        try:
            benji_obj = Benji(self.config)
            for version_uid in version_uids:
                try:
                    benji_obj.unprotect(version_uid)
                except benji.exception.NoChange:
                    logger.warning('Version {} already was unprotected.'.format(version_uid))
        finally:
            if benji_obj:
                benji_obj.close()

    def rm(self, version_uids, force, keep_backend_metadata):
        version_uids = VersionUid.create_from_readables(version_uids)
        disallow_rm_when_younger_than_days = self.config.get('disallowRemoveWhenYounger', types=int)
        benji_obj = None
        try:
            benji_obj = Benji(self.config)
            for version_uid in version_uids:
                benji_obj.rm(
                    version_uid,
                    force=force,
                    disallow_rm_when_younger_than_days=disallow_rm_when_younger_than_days,
                    keep_backend_metadata=keep_backend_metadata)
        finally:
            if benji_obj:
                benji_obj.close()

    def scrub(self, version_uid, percentile):
        version_uid = VersionUid.create_from_readables(version_uid)
        if percentile:
            percentile = int(percentile)
        benji_obj = None
        try:
            benji_obj = Benji(self.config)
            benji_obj.scrub(version_uid, percentile)
        finally:
            if benji_obj:
                benji_obj.close()

    def deep_scrub(self, version_uid, source, percentile):
        version_uid = VersionUid.create_from_readables(version_uid)
        if percentile:
            percentile = int(percentile)
        benji_obj = None
        try:
            benji_obj = Benji(self.config)
            benji_obj.deep_scrub(version_uid, source, percentile)
        finally:
            if benji_obj:
                benji_obj.close()

    def _bulk_scrub(self, method, names, tags, percentile):
        if percentile:
            percentile = int(percentile)
        benji_obj = None
        try:
            benji_obj = Benji(self.config)
            versions = []
            if names:
                for name in names:
                    versions.extend(benji_obj.ls(version_name=name, version_tags=tags))
            else:
                versions.extend(benji_obj.ls(version_tags=tags))
            errors = []
            if percentile and versions:
                # Will always scrub at least one matching version
                versions = random.sample(versions, max(1, int(len(versions) * percentile / 100)))
            if not versions:
                logger.info('No matching versions found.')
            for version in versions:
                try:
                    logging.info('Scrubbing version {} with name {}.'.format(version.uid.readable, version.name))
                    getattr(benji_obj, method)(version.uid)
                except benji.exception.ScrubbingError as exception:
                    logger.error(exception)
                    errors.append(version)
                except:
                    raise
            if errors:
                raise benji.exception.ScrubbingError('One or more version had scrubbing errors: {}.'.format(', '.join(
                    [version.uid.readable for version in errors])))
        finally:
            if benji_obj:
                benji_obj.close()

    def bulk_scrub(self, names, tags, percentile):
        self._bulk_scrub('scrub', names, tags, percentile)

    def bulk_deep_scrub(self, names, tags, percentile):
        self._bulk_scrub('deep_scrub', names, tags, percentile)

    @classmethod
    def _ls_versions_tbl_output(cls, versions):
        tbl = PrettyTable()
        # TODO: number of invalid blocks, used disk space, shared disk space
        tbl.field_names = ['date', 'uid', 'name', 'snapshot_name', 'size', 'block_size', 'valid', 'protected', 'tags']
        tbl.align['name'] = 'l'
        tbl.align['snapshot_name'] = 'l'
        tbl.align['tags'] = 'l'
        tbl.align['size'] = 'r'
        tbl.align['block_size'] = 'r'
        for version in versions:
            tbl.add_row([
                PrettyPrint.local_time(version.date),
                version.uid.readable,
                version.name,
                version.snapshot_name,
                PrettyPrint.bytes(version.size),
                PrettyPrint.bytes(version.block_size),
                version.valid,
                version.protected,
                ",".join(sorted([t.name for t in version.tags])),
            ])
        print(tbl)

    @classmethod
    def _stats_tbl_output(cls, stats):
        tbl = PrettyTable()
        tbl.field_names = [
            'date', 'uid', 'name', 'snapshot_name', 'size', 'block_size', 'read', 'written', 'dedup', 'sparse',
            'duration (s)'
        ]
        tbl.align['uid'] = 'l'
        tbl.align['name'] = 'l'
        tbl.align['snapshot_name'] = 'l'
        tbl.align['size bytes'] = 'r'
        tbl.align['size blocks'] = 'r'
        tbl.align['read'] = 'r'
        tbl.align['written'] = 'r'
        tbl.align['dedup'] = 'r'
        tbl.align['sparse'] = 'r'
        tbl.align['duration (s)'] = 'r'
        for stat in stats:
            augmented_version_uid = '{}{}{}'.format(
                stat.version_uid.readable, ',\nbase {}'.format(stat.base_version_uid.readable)
                if stat.base_version_uid else '', ', hints' if stat.hints_supplied else '')
            tbl.add_row([
                PrettyPrint.local_time(stat.date),
                augmented_version_uid,
                stat.version_name,
                stat.version_snapshot_name,
                PrettyPrint.bytes(stat.version_size),
                PrettyPrint.bytes(stat.version_block_size),
                PrettyPrint.bytes(stat.bytes_read),
                PrettyPrint.bytes(stat.bytes_written),
                PrettyPrint.bytes(stat.bytes_found_dedup),
                PrettyPrint.bytes(stat.bytes_sparse),
                PrettyPrint.duration(stat.duration_seconds),
            ])
        print(tbl)

    def ls(self, name, snapshot_name=None, tags=None, include_blocks=False):
        benji_obj = None
        try:
            benji_obj = Benji(self.config)
            versions = benji_obj.ls(version_name=name, version_snapshot_name=snapshot_name, version_tags=tags)

            if self.machine_output:
                benji_obj.export_any(
                    'versions',
                    versions,
                    sys.stdout,
                    ignore_relationships=[((Version,), ('blocks',))] if not include_blocks else [],
                )
            else:
                self._ls_versions_tbl_output(versions)
        finally:
            if benji_obj:
                benji_obj.close()

    def diff_meta(self, version_uid1, version_uid2):
        """ Output difference between two version in blocks.
        """
        version_uid1 = VersionUid.create_from_readables(version_uid1)
        version_uid2 = VersionUid.create_from_readables(version_uid2)
        # TODO: Feel free to create a default diff format.
        benji_obj = None
        try:
            benji_obj = Benji(self.config)
            # Check if versions exist
            blocks1 = benji_obj.ls_version(version_uid1)
            if not blocks1:
                raise FileNotFoundError('Version {} doesn\'t exist.'.format(version_uid1.readable))
            blocks2 = benji_obj.ls_version(version_uid2)
            if not blocks2:
                raise FileNotFoundError('Version {} doesn\'t exist.'.format(version_uid2.readable))
            max_len = max(len(blocks1), len(blocks2))
            for i in range(max_len):
                b1 = b2 = None
                try:
                    b1 = blocks1.pop(0)
                except IndexError:
                    pass
                try:
                    b2 = blocks2.pop(0)
                except IndexError:
                    pass
                if b1 and b2:
                    assert b1.id == b2.id
                try:
                    if b1.uid == b2.uid:
                        print('SAME      {}'.format(b1.id))
                    elif b1 is None and b2:
                        print('NEW RIGHT {}'.format(b2.id))
                    elif b1 and b2 is None:
                        print('NEW LEFT  {}'.format(b1.id))
                    else:
                        print('DIFF      {}'.format(b1.id))
                except BrokenPipeError:
                    pass
        finally:
            if benji_obj:
                benji_obj.close()

    def stats(self, version_uid, limit=None):
        version_uid = VersionUid.create_from_readables(version_uid)

        if limit:
            limit = int(limit)
            if limit <= 0:
                raise benji.exception.UsageError('Limit has to be a positive integer.')

        benji_obj = None
        try:
            benji_obj = Benji(self.config)
            stats = benji_obj.stats(version_uid, limit)

            if self.machine_output:
                stats = list(stats)  # resolve iterator, otherwise it's not serializable
                benji_obj.export_any(
                    'stats',
                    stats,
                    sys.stdout,
                )
            else:
                self._stats_tbl_output(stats)
        finally:
            if benji_obj:
                benji_obj.close()

    def cleanup(self, full):
        benji_obj = None
        try:
            benji_obj = Benji(self.config)
            if full:
                benji_obj.cleanup_full()
            else:
                benji_obj.cleanup_fast()
        finally:
            if benji_obj:
                benji_obj.close()

    def export(self, version_uids, output_file=None, force=False):
        version_uids = VersionUid.create_from_readables(version_uids)
        benji_obj = None
        try:
            benji_obj = Benji(self.config)
            if output_file is None:
                benji_obj.export(version_uids, sys.stdout)
            else:
                if os.path.exists(output_file) and not force:
                    raise FileExistsError('The output file already exists.')

                with open(output_file, 'w') as f:
                    benji_obj.export(version_uids, f)
        finally:
            if benji_obj:
                benji_obj.close()

    def export_to_backend(self, version_uids, force=False):
        version_uids = VersionUid.create_from_readables(version_uids)
        benji_obj = None
        try:
            benji_obj = Benji(self.config)
            benji_obj.export_to_backend(version_uids, overwrite=force)
        finally:
            if benji_obj:
                benji_obj.close()

    def nbd(self, bind_address, bind_port, read_only):
        benji_obj = None
        try:
            benji_obj = Benji(self.config)
            store = BenjiStore(benji_obj)
            addr = (bind_address, bind_port)
            server = NbdServer(addr, store, read_only)
            logger.info("Starting to serve nbd on %s:%s" % (addr[0], addr[1]))
            server.serve_forever()
        finally:
            if benji_obj:
                benji_obj.close()

    def import_(self, input_file=None):
        benji_obj = None
        try:
            benji_obj = Benji(self.config)
            if input_file is None:
                benji_obj.import_(sys.stdin)
            else:
                with open(input_file, 'r') as f:
                    benji_obj.import_(f)
        finally:
            if benji_obj:
                benji_obj.close()

    def import_from_backend(self, version_uids):
        version_uids = VersionUid.create_from_readables(version_uids)
        benji_obj = None
        try:
            benji_obj = Benji(self.config)
            benji_obj.import_from_backend(version_uids)
        finally:
            if benji_obj:
                benji_obj.close()

    def add_tag(self, version_uid, names):
        version_uid = VersionUid.create_from_readables(version_uid)
        benji_obj = None
        try:
            benji_obj = Benji(self.config)
            for name in names:
                try:
                    benji_obj.add_tag(version_uid, name)
                except benji.exception.NoChange:
                    logger.warning('Version {} already tagged with {}.'.format(version_uid, name))
        finally:
            if benji_obj:
                benji_obj.close()

    def rm_tag(self, version_uid, names):
        version_uid = VersionUid.create_from_readables(version_uid)
        benji_obj = None
        try:
            benji_obj = Benji(self.config)
            for name in names:
                try:
                    benji_obj.rm_tag(version_uid, name)
                except benji.exception.NoChange:
                    logger.warning('Version {} has no tag {}.'.format(version_uid, name))
        finally:
            if benji_obj:
                benji_obj.close()

    def initdb(self):
        Benji(self.config, initdb=True)

    def enforce_retention_policy(self, rules_spec, version_names, dry_run, keep_backend_metadata):
        benji_obj = None
        try:
            benji_obj = Benji(self.config)
            dismissed_version_uids = []
            for version_name in version_names:
                dismissed_version_uids.extend(
                    benji_obj.enforce_retention_policy(
                        version_name=version_name,
                        rules_spec=rules_spec,
                        dry_run=dry_run,
                        keep_backend_metadata=keep_backend_metadata))
            if self.machine_output:
                benji_obj.export_any(
                    'versions', [benji_obj.ls(version_uid=version_uid)[0] for version_uid in dismissed_version_uids],
                    sys.stdout,
                    ignore_relationships=[((Version,), ('blocks',))])
        finally:
            if benji_obj:
                benji_obj.close()


def main():
    parser = argparse.ArgumentParser(
        description='Backup and restore for block devices.', formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('-v', '--verbose', action='store_true', help='Verbose output')
    parser.add_argument('-m', '--machine-output', action='store_true', default=False)
    parser.add_argument('-V', '--version', action='store_true', help='Show version')
    parser.add_argument('-c', '--configfile', default=None, type=str)

    subparsers = parser.add_subparsers()

    # INITDB
    p = subparsers.add_parser(
        'initdb',
        help="Initialize the database by populating tables. This will not delete tables or data if they exist.")
    p.set_defaults(func='initdb')

    # BACKUP
    p = subparsers.add_parser('backup', help="Perform a backup.")
    p.add_argument('source', help='Source (url-like, e.g. file:///dev/sda or rbd://pool/imagename@snapshot)')
    p.add_argument('name', help='Backup name (e.g. the hostname)')
    p.add_argument('-s', '--snapshot-name', default='', help='Snapshot name (e.g. the name of the RBD snapshot)')
    p.add_argument('-r', '--rbd', default=None, help='Hints as RBD JSON format')
    p.add_argument('-f', '--from-version', dest='base_version_uid', default=None, help='Use this version as base')
    p.add_argument(
        '-t',
        '--tag',
        action='append',
        dest='tags',
        metavar='tag',
        default=None,
        help='Tag this verion with the specified tag(s)')
    p.add_argument('-b', '--block-size', type=int, help='Block size to use for this backup in bytes')
    p.set_defaults(func='backup')

    # RESTORE
    p = subparsers.add_parser('restore', help="Restore a given backup to a given target.")
    p.add_argument(
        '-s',
        '--sparse',
        action='store_true',
        help='Restore only existing blocks. Works only with file ' + 'and RBD targets, not with LVM. Faster.')
    p.add_argument('-f', '--force', action='store_true', help='Force overwrite of existing files/devices/images')
    p.add_argument(
        '-M',
        '--metadata-backend-less',
        action='store_true',
        help='Restore directly from data backend without requiring the metadata backend.')
    p.add_argument('version_uid')
    p.add_argument('target', help='Source (URL like, e.g. file:///dev/sda or rbd://pool/imagename)')
    p.set_defaults(func='restore')

    # PROTECT
    p = subparsers.add_parser('protect', help="Protect a backup version. Protected versions cannot be removed.")
    p.add_argument('version_uids', metavar='version_uid', nargs='+', help="Version UID")
    p.set_defaults(func='protect')

    # UNPROTECT
    p = subparsers.add_parser('unprotect', help="Unprotect a backup version. Unprotected versions can be removed.")
    p.add_argument('version_uids', metavar='version_uid', nargs='+', help="Version UID")
    p.set_defaults(func='unprotect')

    # RM
    p = subparsers.add_parser(
        'rm',
        help="Remove the given backup versions. This will only remove meta data and you will have to cleanup after this.")
    p.add_argument(
        '-f',
        '--force',
        action='store_true',
        help="Force removal of version, even if it's younger than the configured disallow_rm_when_younger_than_days.")
    p.add_argument(
        '-k', '--keep-backend-metadata', action='store_true', help='Don\'t delete version\'s metadata in data backend.')
    p.add_argument('version_uids', metavar='version_uid', nargs='+')
    p.set_defaults(func='rm')

    # ENFORCE
    p = subparsers.add_parser('enforce', help="Enforce the given retenion policy on each listed version.")
    p.add_argument('--dry-run', action='store_true', help='Dry run: Only show which versions would be removed.')
    p.add_argument(
        '-k', '--keep-backend-metadata', action='store_true', help='Don\'t delete version\'s metadata in data backend.')
    p.add_argument('rules_spec', help='Retention rules specification')
    p.add_argument('version_names', metavar='version_name', nargs='+')
    p.set_defaults(func='enforce_retention_policy')

    # SCRUB
    p = subparsers.add_parser('scrub', help="Scrub a given backup and check for consistency.")
    p.add_argument(
        '-p',
        '--percentile',
        default=100,
        help="Only check PERCENTILE percent of the blocks (value 0..100). Default: 100")
    p.add_argument('version_uid', help='Version UID')
    p.set_defaults(func='scrub')

    # DEEP-SCRUB
    p = subparsers.add_parser('deep-scrub', help="Deep scrub a given backup and check for consistency.")
    p.add_argument(
        '-s',
        '--source',
        default=None,
        help='Source, optional. If given, check if source matches backup in addition to checksum tests. URL-like format as in backup.')
    p.add_argument(
        '-p',
        '--percentile',
        default=100,
        help="Only check PERCENTILE percent of the blocks (value 0..100). Default: 100")
    p.add_argument('version_uid', help='Version UID')
    p.set_defaults(func='deep_scrub')

    # BULK-SCRUB
    p = subparsers.add_parser('bulk-scrub', help="Bulk deep scrub all matching versions.")
    p.add_argument(
        '-p',
        '--percentile',
        default=100,
        help="Only check PERCENTILE percent of the matching versions (value 0..100). Default: 100")
    p.add_argument(
        '-t',
        '--tag',
        action='append',
        dest='tags',
        metavar='TAG',
        default=None,
        help='Scrub only versions matching this tag.')
    p.add_argument('names', metavar='NAME', nargs='*', help="Version names")
    p.set_defaults(func='bulk_scrub')

    # BULK-DEEP-SCRUB
    p = subparsers.add_parser('bulk-deep-scrub', help="Bulk deep scrub all matching versions.")
    p.add_argument(
        '-t',
        '--tag',
        action='append',
        dest='tags',
        metavar='TAG',
        default=None,
        help='Scrub only versions matching this tag. Multiple use of this option constitutes an OR operation. ')
    p.add_argument(
        '-p',
        '--percentile',
        default=100,
        help="Only check PERCENTILE percent of the matching versions (value 0..100). Default: 100")
    p.add_argument('names', metavar='NAME', nargs='*', help="Version names")
    p.set_defaults(func='bulk_deep_scrub')

    # Export
    p = subparsers.add_parser('export', help='Export the metadata of one or more versions to a file or standard out.')
    p.add_argument('version_uids', metavar='VERSION_UID', nargs='+', help="Version UID")
    p.add_argument('-f', '--force', action='store_true', help='Force overwrite of existing output file')
    p.add_argument(
        '-o', '--output-file', help='Write export into this file (stdout is used if this option isn\'t specified)')
    p.set_defaults(func='export')

    # Import
    p = subparsers.add_parser(
        'import', help='Import the metadata of one or more versions from a file or standard input.')
    p.add_argument('-i', '--input-file', help='Read from this file (stdin is used if this option isn\'t specified)')
    p.set_defaults(func='import_')

    # Export to data backend
    p = subparsers.add_parser('export-to-backend', help='Export metadata of one or more versions to the data backend')
    p.add_argument('version_uids', metavar='VERSION_UID', nargs='+', help="Version UID")
    p.add_argument('-f', '--force', action='store_true', help='Force overwrite of existing metadata in data backend')
    p.set_defaults(func='export_to_backend')

    # Import from data backend
    p = subparsers.add_parser(
        'import-from-backend', help="Import metadata of one ore more versions from the data backend")
    p.add_argument('version_uids', metavar='VERSION_UID', nargs='+', help="Version UID")
    p.set_defaults(func='import_from_backend')

    # CLEANUP
    p = subparsers.add_parser('cleanup', help="Clean unreferenced blobs.")
    p.add_argument(
        '-f',
        '--full',
        action='store_true',
        default=False,
        help='Do a full cleanup. This will read the full metadata from the data backend (i.e. backup storage) '
        'and compare it to the metadata in the metadata backend. Unused data will then be deleted. '
        'This is a slow, but complete process. A full cleanup must not run in parallel to ANY other jobs.')
    p.set_defaults(func='cleanup')

    # LS
    p = subparsers.add_parser('ls', help="List existing backups.")
    p.add_argument('name', nargs='?', default=None, help='Show versions for this name only')
    p.add_argument('-s', '--snapshot-name', default=None, help="Limit output to this SNAPSHOT_NAME")
    p.add_argument(
        '-t',
        '--tag',
        action='append',
        dest='tags',
        metavar='TAG',
        default=None,
        help='Limit output to this TAG. Multiple use constitutes an OR operation.')
    p.add_argument('--include-blocks', default=False, action='store_true', help='Include blocks in output')
    p.set_defaults(func='ls')

    # STATS
    p = subparsers.add_parser('stats', help="Show statistics")
    p.add_argument('version_uid', nargs='?', default=None, help='Show statistics for this version')
    p.add_argument('-l', '--limit', default=None, help="Limit output to this number (default: unlimited)")
    p.set_defaults(func='stats')

    # diff-meta
    p = subparsers.add_parser('diff-meta', help="Output a diff between two versions")
    p.add_argument('version_uid1', help='Left version')
    p.add_argument('version_uid2', help='Right version')
    p.set_defaults(func='diff_meta')

    # NBD
    p = subparsers.add_parser('nbd', help="Start an nbd server")
    p.add_argument('-a', '--bind-address', default='127.0.0.1', help="Bind to this ip address (default: 127.0.0.1)")
    p.add_argument('-p', '--bind-port', default=10809, help="Bind to this port (default: 10809)")
    p.add_argument(
        '-r',
        '--read-only',
        action='store_true',
        default=False,
        help='Read only if set, otherwise a copy on write backup is created.')
    p.set_defaults(func='nbd')

    # ADD TAG
    p = subparsers.add_parser('add-tag', help="Add a named tag to a backup version.")
    p.add_argument('version_uid')
    p.add_argument('names', metavar='NAME', nargs='+')
    p.set_defaults(func='add_tag')

    # REMOVE TAG
    p = subparsers.add_parser('rm-tag', help="Remove a named tag from a backup version.")
    p.add_argument('version_uid')
    p.add_argument('names', metavar='NAME', nargs='+')
    p.set_defaults(func='rm_tag')

    args = parser.parse_args()

    if args.version:
        print(__version__)
        exit(os.EX_OK)

    if not hasattr(args, 'func'):
        parser.print_usage()
        exit(os.EX_USAGE)

    if args.verbose:
        console_level = logging.DEBUG
    else:
        console_level = logging.INFO

    if args.configfile is not None and args.configfile != '':
        try:
            cfg = open(args.configfile, 'r', encoding='utf-8').read()
        except FileNotFoundError:
            logger.error('File {} not found.'.format(args.configfile))
            exit(os.EX_USAGE)
        config = Config(cfg=cfg)
    else:
        config = Config()

    # logging ERROR only when machine output is selected
    if args.machine_output:
        init_logging(config.get('logFile', types=(str, type(None))), logging.ERROR)
    else:
        init_logging(config.get('logFile', types=(str, type(None))), console_level)

    commands = Commands(args.machine_output, config)
    func = getattr(commands, args.func)

    # Pass over to function
    func_args = dict(args._get_kwargs())
    del func_args['configfile']
    del func_args['func']
    del func_args['verbose']
    del func_args['version']
    del func_args['machine_output']

    # From most specific to least specific
    exit_code_list = [
        {
            'exception': benji.exception.UsageError,
            'msg': 'Usage error',
            'exit_code': os.EX_USAGE
        },
        {
            'exception': benji.exception.AlreadyLocked,
            'msg': 'Already locked error',
            'exit_code': os.EX_NOPERM
        },
        {
            'exception': benji.exception.InternalError,
            'msg': 'Internal error',
            'exit_code': os.EX_SOFTWARE
        },
        {
            'exception': benji.exception.ConfigurationError,
            'msg': 'Configuration error',
            'exit_code': os.EX_CONFIG
        },
        {
            'exception': benji.exception.InputDataError,
            'msg': 'Input data error',
            'exit_code': os.EX_DATAERR
        },
        {
            'exception': benji.exception.ScrubbingError,
            'msg': 'Scrubbing error',
            'exit_code': os.EX_DATAERR
        },
        {
            'exception': PermissionError,
            'msg': 'Already locked error',
            'exit_code': os.EX_NOPERM
        },
        {
            'exception': FileExistsError,
            'msg': 'Already exists',
            'exit_code': os.EX_CANTCREAT
        },
        {
            'exception': FileNotFoundError,
            'msg': 'Not found',
            'exit_code': os.EX_NOINPUT
        },
        {
            'exception': EOFError,
            'msg': 'I/O error',
            'exit_code': os.EX_IOERR
        },
        {
            'exception': IOError,
            'msg': 'I/O error',
            'exit_code': os.EX_IOERR
        },
        {
            'exception': OSError,
            'msg': 'Not found',
            'exit_code': os.EX_OSERR
        },
        {
            'exception': ConnectionError,
            'msg': 'I/O error',
            'exit_code': os.EX_IOERR
        },
        {
            'exception': LookupError,
            'msg': 'Not found',
            'exit_code': os.EX_NOINPUT
        },
        {
            'exception': BaseException,
            'msg': 'Other exception',
            'exit_code': os.EX_SOFTWARE
        },
    ]

    try:
        logger.debug('backup.{0}(**{1!r})'.format(args.func, func_args))
        func(**func_args)
        exit(0)
    except SystemExit:
        raise
    except BaseException as exception:
        for case in exit_code_list:
            if isinstance(exception, case['exception']):
                logger.debug(case['msg'], exc_info=True)
                logger.error(str(exception))
                exit(case['exit_code'])


if __name__ == '__main__':
    main()
