#!/usr/bin/env python
# -*- encoding: utf-8 -*-

import argparse
import fileinput
import logging
import os
import sys

import pkg_resources
from dateutil import tz
from prettytable import PrettyTable

import benji.exception
from benji.benji import Benji
from benji.config import Config
from benji.logging import logger, init_logging
from benji.meta_backend import Version, VersionUid
from benji.utils import hints_from_rbd_diff, parametrized_hash_function, human_readable_duration

__version__ = pkg_resources.get_distribution('benji').version


class Commands:
    """Proxy between CLI calls and actual backup code."""

    def __init__(self, machine_output, config):
        self.machine_output = machine_output
        self.config = config

    def backup(self, name, snapshot_name, source, rbd, from_version_uid, block_size=None, tags=None):
        from_version_uid = VersionUid.create_from_readables(from_version_uid)
        benji = None
        try:
            benji = Benji(self.config, block_size=block_size)
            hints = None
            if rbd:
                data = ''.join([line for line in fileinput.input(rbd).readline()])
                hints = hints_from_rbd_diff(data)
            backup_version_uid = benji.backup(name, snapshot_name, source, hints, from_version_uid, tags)
            if self.machine_output:
                benji._meta_backend.export_any('versions',
                                               [benji._meta_backend.get_versions(version_uid=backup_version_uid)],
                                               sys.stdout,
                                               ignore_relationships=[((Version,), ('blocks',))]
                                               )
        finally:
            if benji:
                benji.close()

    def restore(self, version_uid, target, sparse, force):
        version_uid = VersionUid.create_from_readables(version_uid)
        benji = None
        try:
            benji = Benji(self.config)
            benji.restore(version_uid, target, sparse, force)
        finally:
            if benji:
                benji.close()

    def protect(self, version_uids):
        version_uids = VersionUid.create_from_readables(version_uids)
        benji = None
        try:
            benji = Benji(self.config)
            for version_uid in version_uids:
                try:
                    benji.protect(version_uid)
                except benji.exception.NoChange:
                    logger.warning('Version {} already was protected.'.format(version_uid))
        finally:
            if benji:
                benji.close()

    def unprotect(self, version_uids):
        version_uids = VersionUid.create_from_readables(version_uids)
        benji = None
        try:
            benji = Benji(self.config)
            for version_uid in version_uids:
                try:
                    benji.unprotect(version_uid)
                except benji.exception.NoChange:
                    logger.warning('Version {} already was unprotected.'.format(version_uid))
        finally:
            if benji:
                benji.close()

    def rm(self, version_uids, force, keep_backend_metadata):
        version_uids = VersionUid.create_from_readables(version_uids)
        disallow_rm_when_younger_than_days = self.config.get('disallowRemoveWhenYounger', types=int)
        benji = None
        try:
            benji = Benji(self.config)
            for version_uid in version_uids:
                benji.rm(version_uid, force=force,
                        disallow_rm_when_younger_than_days=disallow_rm_when_younger_than_days,
                        keep_backend_metadata=keep_backend_metadata)
        finally:
            if benji:
                benji.close()

    def scrub(self, version_uid, percentile):
        version_uid = VersionUid.create_from_readables(version_uid)
        if percentile:
            percentile = int(percentile)
        benji = None
        try:
            benji = Benji(self.config)
            benji.scrub(version_uid, percentile)
        finally:
            if benji:
                benji.close()

    def deep_scrub(self, version_uid, source, percentile):
        version_uid = VersionUid.create_from_readables(version_uid)
        if percentile:
            percentile = int(percentile)
        benji = None
        try:
            benji = Benji(self.config)
            benji.deep_scrub(version_uid, source, percentile)
        finally:
            if benji:
                benji.close()

    @staticmethod
    def _local_time(date):
        return date.replace(tzinfo=tz.tzutc()).astimezone(tz.tzlocal()).strftime("%Y-%m-%dT%H:%M:%S")

    @classmethod
    def _ls_versions_tbl_output(cls, versions):
        tbl = PrettyTable()
        # TODO: number of invalid blocks, used disk space, shared disk space
        tbl.field_names = ['date', 'uid', 'name', 'snapshot_name', 'size', 'block_size',
                           'valid', 'protected', 'tags']
        tbl.align['name'] = 'l'
        tbl.align['snapshot_name'] = 'l'
        tbl.align['tags'] = 'l'
        tbl.align['size'] = 'r'
        tbl.align['block_size'] = 'r'
        for version in versions:
            tbl.add_row([
                cls._local_time(version.date),
                version.uid.readable,
                version.name,
                version.snapshot_name,
                version.size,
                version.block_size,
                version.valid,
                version.protected,
                ",".join(sorted([t.name for t in version.tags])),
                ])
        print(tbl)

    @classmethod
    def _stats_tbl_output(cls, stats):
        tbl = PrettyTable()
        tbl.field_names = ['date', 'uid', 'name', 'snapshot_name', 'size', 'block_size',
                           'bytes read', 'blocks read', 'bytes written', 'blocks written',
                           'bytes dedup', 'blocks dedup', 'bytes sparse', 'blocks sparse', 'duration (s)']
        tbl.align['name'] = 'l'
        tbl.align['snapshot_name'] = 'l'
        tbl.align['size bytes'] = 'r'
        tbl.align['size blocks'] = 'r'
        tbl.align['bytes read'] = 'r'
        tbl.align['blocks read'] = 'r'
        tbl.align['bytes written'] = 'r'
        tbl.align['blocks written'] = 'r'
        tbl.align['bytes dedup'] = 'r'
        tbl.align['blocks dedup'] = 'r'
        tbl.align['bytes sparse'] = 'r'
        tbl.align['blocks sparse'] = 'r'
        tbl.align['duration (s)'] = 'r'
        for stat in stats:
            tbl.add_row([
                cls._local_time(stat.date),
                stat.version_uid.readable,
                stat.version_name,
                stat.version_snapshot_name,
                stat.version_size,
                stat.version_block_size,
                stat.bytes_read,
                stat.blocks_read,
                stat.bytes_written,
                stat.blocks_written,
                stat.bytes_found_dedup,
                stat.blocks_found_dedup,
                stat.bytes_sparse,
                stat.blocks_sparse,
                human_readable_duration(stat.duration_seconds),
                ])
        print(tbl)

    def ls(self, name, snapshot_name=None, tag=None, include_blocks=False):
        benji = None
        try:
            benji = Benji(self.config)
            versions = benji.ls(version_name=name, version_snapshot_name=snapshot_name)

            if tag:
                versions = [v for v in versions if tag in [t.name for t in v.tags]]

            if self.machine_output:
                benji._meta_backend.export_any('versions',
                                               versions,
                                               sys.stdout,
                                               ignore_relationships=[((Version,), ('blocks',))] if not include_blocks else [],
                                               )
            else:
                self._ls_versions_tbl_output(versions)
        finally:
            if benji:
                benji.close()

    def diff_meta(self, version_uid1, version_uid2):
        """ Output difference between two version in blocks.
        """
        version_uid1 = VersionUid.create_from_readables(version_uid1)
        version_uid2 = VersionUid.create_from_readables(version_uid2)
        # TODO: Feel free to create a default diff format.
        benji = None
        try:
            benji = Benji(self.config)
            blocks1 = benji.ls_version(version_uid1)
            blocks2 = benji.ls_version(version_uid2)
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
            if benji:
                benji.close()

    def stats(self, version_uid, limit=None):
        version_uid = VersionUid.create_from_readables(version_uid)

        if limit:
            limit = int(limit)
            if limit <= 0:
                raise benji.UsageError('Limit has to be a positive integer.')

        benji = None
        try:
            benji = Benji(self.config)
            stats = benji.stats(version_uid, limit)

            if self.machine_output:
                stats = list(stats) # resolve iterator, otherwise it's not serializable
                benji._meta_backend.export_any('stats',
                                               stats,
                                               sys.stdout,
                                               )
            else:
                self._stats_tbl_output(stats)
        finally:
            if benji:
                benji.close()

    def cleanup(self, full):
        benji = None
        try:
            benji = Benji(self.config)
            if full:
                benji.cleanup_full()
            else:
                benji.cleanup_fast()
        finally:
            if benji:
                benji.close()

    def export(self, version_uids, output_file=None, force=False):
        version_uids = VersionUid.create_from_readables(version_uids)
        benji = None
        try:
            benji = Benji(self.config)
            if output_file is None:
                benji.export(version_uids, sys.stdout)
            else:
                if os.path.exists(output_file) and not force:
                    raise FileExistsError('The output file already exists.')

                with open(output_file, 'w') as f:
                    benji.export(version_uids, f)
        finally:
            if benji:
                benji.close()

    def export_to_backend(self, version_uids, force=False):
        version_uids = VersionUid.create_from_readables(version_uids)
        benji = None
        try:
            benji = Benji(self.config)
            benji.export_to_backend(version_uids, overwrite=force)
        finally:
            if benji:
                benji.close()

    def nbd(self, bind_address, bind_port, read_only):
        from benji.nbd.nbdserver import Server as NbdServer
        from benji.nbd.nbd import BenjiStore
        benji = None
        try:
            benji = Benji(self.config)
            hash_function = parametrized_hash_function(self.config.get('hashFunction', types=str))
            cache_dir = self.config.get('nbd.cacheDirectory', types=str)
            store = BenjiStore(benji, cachedir=cache_dir, hash_function=hash_function)
            addr = (bind_address, bind_port)
            server = NbdServer(addr, store, read_only)
            logger.info("Starting to serve nbd on %s:%s" % (addr[0], addr[1]))
            logger.info("You may now start")
            logger.info("  nbd-client -l %s -p %s" % (addr[0], addr[1]))
            logger.info("and then get the backup via")
            logger.info("  modprobe nbd")
            logger.info("  nbd-client -N <version> %s -p %s /dev/nbd0" % (addr[0], addr[1]))
            server.serve_forever()
        finally:
            if benji:
                benji.close()

    def import_(self, input_file=None):
        benji = None
        try:
            benji = Benji(self.config)
            if input_file is None:
                benji.import_(sys.stdin)
            else:
                with open(input_file, 'r') as f:
                    benji.import_(f)
        finally:
            if benji:
                benji.close()

    def import_from_backend(self, version_uids):
        version_uids = VersionUid.create_from_readables(version_uids)
        benji = None
        try:
            benji = Benji(self.config)
            benji.import_from_backend(version_uids)
        finally:
            if benji:
                benji.close()

    def add_tag(self, version_uid, names):
        version_uid = VersionUid.create_from_readables(version_uid)
        benji = None
        try:
            benji = Benji(self.config)
            for name in names:
                try:
                    benji.add_tag(version_uid, name)
                except benji.exception.NoChange:
                    logger.warning('Version {} already tagged with {}.'.format(version_uid, name))
        finally:
            if benji:
                benji.close()

    def rm_tag(self, version_uid, names):
        version_uid = VersionUid.create_from_readables(version_uid)
        benji = None
        try:
            benji = Benji(self.config)
            for name in names:
                try:
                    benji.rm_tag(version_uid, name)
                except benji.exception.NoChange:
                    logger.warning('Version {} has no tag {}.'.format(version_uid, name))
        finally:
            if benji:
                benji.close()

    def initdb(self):
        Benji(self.config, initdb=True)

    def enforce_retention_policy(self, rules_spec, version_names, dry_run, keep_backend_metadata):
        benji = None
        try:
            benji = Benji(self.config)
            dismissed_version_uids = []
            for version_name in version_names:
                dismissed_version_uids.extend(benji.enforce_retention_policy(version_name=version_name,
                                                                        rules_spec=rules_spec,
                                                                        dry_run=dry_run,
                                                                        keep_backend_metadata=keep_backend_metadata))
            if self.machine_output:
                benji._meta_backend.export_any('versions',
                                               [benji._meta_backend.get_versions(version_uid=version_uid)[0]
                                                                        for version_uid in dismissed_version_uids],
                                               sys.stdout,
                                               ignore_relationships=[((Version,), ('blocks',))]
                                               )
        finally:
                if benji:
                    benji.close()

def main():
    parser = argparse.ArgumentParser(
        description='Backup and restore for block devices.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-v', '--verbose', action='store_true', help='Verbose output')
    parser.add_argument(
        '-m', '--machine-output', action='store_true', default=False)
    parser.add_argument(
        '-V', '--version', action='store_true', help='Show version')
    parser.add_argument(
        '-c', '--configfile', default=None, type=str)

    subparsers = parser.add_subparsers()

    # INITDB
    p = subparsers.add_parser(
        'initdb',
        help="Initialize the database by populating tables. This will not delete tables or data if they exist.")
    p.set_defaults(func='initdb')

    # BACKUP
    p = subparsers.add_parser(
        'backup',
        help="Perform a backup.")
    p.add_argument(
        'source',
        help='Source (url-like, e.g. file:///dev/sda or rbd://pool/imagename@snapshot)')
    p.add_argument(
        'name',
        help='Backup name (e.g. the hostname)')
    p.add_argument('-s', '--snapshot-name', default='', help='Snapshot name (e.g. the name of the RBD snapshot)')
    p.add_argument('-r', '--rbd', default=None, help='Hints as RBD JSON format')
    p.add_argument('-f', '--from-version', dest='from_version_uid', default=None, help='Use this version as base')
    p.add_argument('-t', '--tag', nargs='*',  dest='tags', metavar='tag', default=None,
                   help='Tag this verion with the specified tag(s)')
    p.add_argument('-b', '--block-size', type=int, help='Block size to use for this backup in bytes')
    p.set_defaults(func='backup')

    # RESTORE
    p = subparsers.add_parser(
        'restore',
        help="Restore a given backup to a given target.")
    p.add_argument('-s', '--sparse', action='store_true', help='Restore only existing blocks. Works only with file '
                                                               + 'and RBD targets, not with LVM. Faster.')
    p.add_argument('-f', '--force', action='store_true', help='Force overwrite of existing files/devices/images')
    p.add_argument('version_uid')
    p.add_argument('target',
        help='Source (URL like, e.g. file:///dev/sda or rbd://pool/imagename)')
    p.set_defaults(func='restore')

    # PROTECT
    p = subparsers.add_parser(
        'protect',
        help="Protect a backup version. Protected versions cannot be removed.")
    p.add_argument('version_uids', metavar='version_uid', nargs='+', help="Version UID")
    p.set_defaults(func='protect')

    # UNPROTECT
    p = subparsers.add_parser(
        'unprotect',
        help="Unprotect a backup version. Unprotected versions can be removed.")
    p.add_argument('version_uids', metavar='version_uid', nargs='+', help="Version UID")
    p.set_defaults(func='unprotect')

    # RM
    p = subparsers.add_parser(
        'rm',
        help="Remove the given backup versions. This will only remove meta data and you will have to cleanup after this.")
    p.add_argument('-f', '--force', action='store_true', help="Force removal of version, even if it's younger than the configured disallow_rm_when_younger_than_days.")
    p.add_argument('-K', '--keep-backend-metadata', action='store_true', help='Don\'t delete version\'s metadata in data backend.')
    p.add_argument('version_uids', metavar='version_uid', nargs='+')
    p.set_defaults(func='rm')

    # ENFORCE
    p = subparsers.add_parser(
        'enforce',
        help="Enforce the given retenion policy on each listed version.")
    p.add_argument('--dry-run', action='store_true', help='Dry run: Only show which versions would be removed.')
    p.add_argument('-K', '--keep-backend-metadata', action='store_true', help='Don\'t delete version\'s metadata in data backend.')
    p.add_argument('rules_spec', help='Retention rules specification')
    p.add_argument('version_names', metavar='version_name', nargs='+')
    p.set_defaults(func='enforce_retention_policy')

    # SCRUB
    p = subparsers.add_parser(
        'scrub',
        help="Scrub a given backup and check for consistency.")
    p.add_argument('-p', '--percentile', default=100,
        help="Only check PERCENTILE percent of the blocks (value 0..100). Default: 100")
    p.add_argument('version_uid', help='Version UID')
    p.set_defaults(func='scrub')

    # DEEP-SCRUB
    p = subparsers.add_parser(
        'deep-scrub',
        help="Deep scrub a given backup and check for consistency.")
    p.add_argument('-s', '--source', default=None,
                   help='Source, optional. If given, check if source matches backup in addition to checksum tests. URL-like format as in backup.')
    p.add_argument('-p', '--percentile', default=100,
                   help="Only check PERCENTILE percent of the blocks (value 0..100). Default: 100")
    p.add_argument('version_uid', help='Version UID')
    p.set_defaults(func='deep_scrub')

    # Export
    p = subparsers.add_parser(
        'export',
        help='Export the metadata of one or more versions to a file or standard out.')
    p.add_argument('version_uids', metavar='version_uid', nargs='+', help="Version UID")
    p.add_argument('-f', '--force', action='store_true', help='Force overwrite of existing output file')
    p.add_argument('-o', '--output-file', help='Write export into this file (stdout is used if this option isn\'t specified)')
    p.set_defaults(func='export')

    # Import
    p = subparsers.add_parser(
        'import',
        help='Import the metadata of one or more versions from a file or standard input.')
    p.add_argument('-i', '--input-file', help='Read from this file (stdin is used if this option isn\'t specified)')
    p.set_defaults(func='import_')

    # Export to data backend
    p = subparsers.add_parser(
        'export-to-backend',
        help='Export metadata of one or more versions to the data backend')
    p.add_argument('version_uids', metavar='version_uid', nargs='+', help="Version UID")
    p.add_argument('-f', '--force', action='store_true', help='Force overwrite of existing metadata in data backend')
    p.set_defaults(func='export_to_backend')

    # Import from data backend
    p = subparsers.add_parser(
        'import-from-backend',
        help="Import metadata of one ore more versions from the data backend")
    p.add_argument('version_uids', metavar='version_uid', nargs='+', help="Version UID")
    p.set_defaults(func='import_from_backend')

    # CLEANUP
    p = subparsers.add_parser(
        'cleanup',
        help="Clean unreferenced blobs.")
    p.add_argument(
        '-f', '--full', action='store_true', default=False,
        help='Do a full cleanup. This will read the full metadata from the data backend (i.e. backup storage) '
             'and compare it to the metadata in the meta backend. Unused data will then be deleted. '
             'This is a slow, but complete process. A full cleanup must not run in parallel to ANY other jobs.')
    p.set_defaults(func='cleanup')

    # LS
    p = subparsers.add_parser(
        'ls',
        help="List existing backups.")
    p.add_argument('name', nargs='?', default=None, help='Show versions for this name only')
    p.add_argument('-s', '--snapshot-name', default=None,
            help="Limit output to this snapshot name")
    p.add_argument('-t', '--tag', default=None,
            help="Limit output to this tag")
    p.add_argument('--include-blocks', default=False, action='store_true',
            help='Include blocks in output')
    p.set_defaults(func='ls')

    # STATS
    p = subparsers.add_parser(
        'stats',
        help="Show statistics")
    p.add_argument('version_uid', nargs='?', default=None, help='Show statistics for this version')
    p.add_argument('-l', '--limit', default=None,
            help="Limit output to this number (default: unlimited)")
    p.set_defaults(func='stats')

    # diff-meta
    p = subparsers.add_parser(
        'diff-meta',
        help="Output a diff between two versions")
    p.add_argument('version_uid1', help='Left version')
    p.add_argument('version_uid2', help='Right version')
    p.set_defaults(func='diff_meta')

    # NBD
    p = subparsers.add_parser(
        'nbd',
        help="Start an nbd server")
    p.add_argument('-a', '--bind-address', default='127.0.0.1',
            help="Bind to this ip address (default: 127.0.0.1)")
    p.add_argument('-p', '--bind-port', default=10809,
            help="Bind to this port (default: 10809)")
    p.add_argument(
        '-r', '--read-only', action='store_true', default=False,
        help='Read only if set, otherwise a copy on write backup is created.')
    p.set_defaults(func='nbd')

    # ADD TAG
    p = subparsers.add_parser(
        'add-tag',
        help="Add a named tag to a backup version.")
    p.add_argument('version_uid')
    p.add_argument('names', metavar='name', nargs='+')
    p.set_defaults(func='add_tag')

    # REMOVE TAG
    p = subparsers.add_parser(
        'rm-tag',
        help="Remove a named tag from a backup version.")
    p.add_argument('version_uid')
    p.add_argument('names', metavar='name', nargs='+')
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
        init_logging(config.get('logFile', types=str), logging.ERROR)
    else:
        init_logging(config.get('logFile', types=str), console_level)

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
        {'exception': benji.exception.UsageError, 'msg': 'Usage error', 'exit_code': os.EX_USAGE},
        {'exception': benji.exception.AlreadyLocked, 'msg': 'Already locked error', 'exit_code': os.EX_NOPERM},
        {'exception': benji.exception.InternalError, 'msg': 'Internal error', 'exit_code': os.EX_SOFTWARE},
        {'exception': benji.exception.ConfigurationError, 'msg': 'Configuration error', 'exit_code': os.EX_CONFIG},
        {'exception': benji.exception.InputDataError, 'msg': 'Input data error', 'exit_code': os.EX_DATAERR},
        {'exception': PermissionError, 'msg': 'Already locked error', 'exit_code': os.EX_NOPERM},
        {'exception': FileExistsError, 'msg': 'Already exists', 'exit_code': os.EX_CANTCREAT},
        {'exception': FileNotFoundError, 'msg': 'Not found', 'exit_code': os.EX_NOINPUT},
        {'exception': EOFError, 'msg': 'I/O error', 'exit_code': os.EX_IOERR},
        {'exception': IOError, 'msg': 'I/O error', 'exit_code': os.EX_IOERR},
        {'exception': OSError, 'msg': 'Not found', 'exit_code': os.EX_OSERR},
        {'exception': ConnectionError, 'msg': 'I/O error', 'exit_code': os.EX_IOERR},
        {'exception': LookupError, 'msg': 'Not found', 'exit_code': os.EX_NOINPUT},
        {'exception': BaseException, 'msg': 'Other exception', 'exit_code': os.EX_SOFTWARE},
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
