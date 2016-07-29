#!/usr/bin/env python
# -*- encoding: utf-8 -*-

from backy2.config import Config as _Config
from backy2.logging import logger, init_logging
from backy2.utils import hints_from_rbd_diff, backy_from_config
from functools import partial
from io import StringIO
from prettytable import PrettyTable
import argparse
import fileinput
import hashlib
import logging
import sys


import pkg_resources
__version__ = pkg_resources.get_distribution('backy2').version


class Commands():
    """Proxy between CLI calls and actual backup code."""

    def __init__(self, machine_output, Config):
        self.machine_output = machine_output
        self.Config = Config
        self.backy = backy_from_config(Config)


    def backup(self, name, source, rbd, from_version):
        backy = self.backy()
        hints = None
        if rbd:
            data = ''.join([line for line in fileinput.input(rbd).readline()])
            hints = hints_from_rbd_diff(data)
        backy.backup(name, source, hints, from_version)
        backy.close()


    def restore(self, version_uid, target, sparse, force):
        backy = self.backy()
        backy.restore(version_uid, target, sparse, force)
        backy.close()


    def rm(self, version_uid, force):
        config_DEFAULTS = self.Config(section='DEFAULTS')
        disallow_rm_when_younger_than_days = int(config_DEFAULTS.get('disallow_rm_when_younger_than_days', '0'))
        backy = self.backy()
        backy.rm(version_uid, force, disallow_rm_when_younger_than_days)
        backy.close()


    def scrub(self, version_uid, source, percentile):
        if percentile:
            percentile = int(percentile)
        backy = self.backy()
        state = backy.scrub(version_uid, source, percentile)
        backy.close()
        if not state:
            exit(20)


    def _ls_blocks_tbl_output(self, blocks):
        tbl = PrettyTable()
        tbl.field_names = ['id', 'date', 'uid', 'size', 'valid']
        tbl.align['id'] = 'r'
        tbl.align['size'] = 'r'
        for block in blocks:
            tbl.add_row([
                block.id,
                block.date,
                block.uid,
                block.size,
                int(block.valid),
                ])
        print(tbl)


    def _ls_blocks_machine_output(self, blocks):
        field_names = ['type', 'id', 'date', 'uid', 'size', 'valid']
        print(' '.join(field_names))
        for block in blocks:
            print(' '.join(map(str, [
                'block',
                block.id,
                block.date,
                block.uid,
                block.size,
                int(block.valid),
                ])))


    def _ls_versions_tbl_output(self, versions):
        tbl = PrettyTable()
        # TODO: number of invalid blocks, used disk space, shared disk space
        tbl.field_names = ['date', 'name', 'size', 'size_bytes', 'uid',
                'version valid']
        tbl.align['name'] = 'l'
        tbl.align['size'] = 'r'
        tbl.align['size_bytes'] = 'r'
        for version in versions:
            tbl.add_row([
                version.date,
                version.name,
                version.size,
                version.size_bytes,
                version.uid,
                int(version.valid),
                ])
        print(tbl)


    def _ls_versions_machine_output(self, versions):
        field_names = ['type', 'date', 'size', 'size_bytes', 'uid', 'version valid', 'name']
        print(' '.join(field_names))
        for version in versions:
            print(' '.join(map(str, [
                'version',
                version.date,
                version.name,
                version.size,
                version.size_bytes,
                version.uid,
                int(version.valid),
                ])))


    def _stats_tbl_output(self, stats):
        tbl = PrettyTable()
        tbl.field_names = ['date', 'uid', 'name', 'size bytes', 'size blocks',
                'bytes read', 'blocks read', 'bytes written', 'blocks written',
                'bytes dedup', 'blocks dedup', 'bytes sparse', 'blocks sparse',
                'duration (s)']
        tbl.align['name'] = 'l'
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
                stat.date,
                stat.version_uid,
                stat.version_name,
                stat.version_size_bytes,
                stat.version_size_blocks,
                stat.bytes_read,
                stat.blocks_read,
                stat.bytes_written,
                stat.blocks_written,
                stat.bytes_found_dedup,
                stat.blocks_found_dedup,
                stat.bytes_sparse,
                stat.blocks_sparse,
                stat.duration_seconds,
                ])
        print(tbl)


    def _stats_machine_output(self, stats):
        field_names = ['type', 'date', 'uid', 'name', 'size bytes', 'size blocks',
                'bytes read', 'blocks read', 'bytes written', 'blocks written',
                'bytes dedup', 'blocks dedup', 'bytes sparse', 'blocks sparse',
                'duration (s)']
        print(' '.join(field_names))
        for stat in stats:
            print(' '.join(map(str, [
                'statistics',
                stat.date,
                stat.version_uid,
                stat.version_name,
                stat.version_size_bytes,
                stat.version_size_blocks,
                stat.bytes_read,
                stat.blocks_read,
                stat.bytes_written,
                stat.blocks_written,
                stat.bytes_found_dedup,
                stat.blocks_found_dedup,
                stat.bytes_sparse,
                stat.blocks_sparse,
                stat.duration_seconds,
                ])))


    def ls(self, version_uid):
        backy = self.backy()
        if version_uid:
            blocks = backy.ls_version(version_uid)
            if self.machine_output:
                self._ls_blocks_machine_output(blocks)
            else:
                self._ls_blocks_tbl_output(blocks)
        else:
            versions = backy.ls()
            if self.machine_output:
                self._ls_versions_machine_output(versions)
            else:
                self._ls_versions_tbl_output(versions)
        backy.close()


    def diff_meta(self, version_uid1, version_uid2):
        """ Output difference between two version in blocks.
        """
        # TODO: Feel free to create a default diff format.
        backy = self.backy()
        blocks1 = backy.ls_version(version_uid1)
        blocks2 = backy.ls_version(version_uid2)
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
        backy.close()


    def stats(self, version_uid, limit=None):
        backy = self.backy()
        if limit is not None:
            limit = int(limit)
        stats = backy.stats(version_uid, limit)
        if self.machine_output:
            self._stats_machine_output(stats)
        else:
            self._stats_tbl_output(stats)
        backy.close()


    def cleanup(self, full, prefix=None):
        backy = self.backy()
        if full:
            backy.cleanup_full(prefix)
        else:
            backy.cleanup_fast()
        backy.close()


    def export(self, version_uid, filename='-'):
        backy = self.backy()
        if filename == '-':
            f = StringIO()
            backy.export(version_uid, f)
            f.seek(0)
            print(f.read())
            f.close()
        else:
            with open(filename, 'w') as f:
                backy.export(version_uid, f)
        backy.close()


    def nbd(self, version_uid, bind_address, bind_port, read_only):
        try:
            from backy2.enterprise.nbdserver import Server as NbdServer
            from backy2.enterprise.nbd import BackyStore
        except ImportError:
            logger.error('NBD is available in the Enterprise Version only.')
            sys.exit(21)
        backy = self.backy()
        config_NBD = self.Config(section='NBD')
        config_DEFAULTS = self.Config(section='DEFAULTS')
        hash_function = getattr(hashlib, config_DEFAULTS.get('hash_function', 'sha512'))
        store = BackyStore(
                backy, cachedir=config_NBD.get('cachedir'),
                hash_function=hash_function,
                )
        addr = (bind_address, bind_port)
        server = NbdServer(addr, store, read_only)
        logger.info("Starting to serve nbd on %s:%s" % (addr[0], addr[1]))
        logger.info("You may now start")
        logger.info("  nbd-client -l %s -p %s" % (addr[0], addr[1]))
        logger.info("and then get the backup via")
        logger.info("  modprobe nbd")
        logger.info("  nbd-client -N <version> %s -p %s /dev/nbd0" % (addr[0], addr[1]))
        server.serve_forever()


    def import_(self, filename='-'):
        backy = self.backy()
        try:
            if filename=='-':
                backy.import_(sys.stdin)
            else:
                with open(filename, 'r') as f:
                    backy.import_(f)
        except KeyError as e:
            logger.error(str(e))
            exit(22)
        except ValueError as e:
            logger.error(str(e))
            exit(23)
        finally:
            backy.close()


def main():
    parser = argparse.ArgumentParser(
        description='Backup and restore for block devices.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-v', '--verbose', action='store_true', help='verbose output')
    parser.add_argument(
        '-m', '--machine-output', action='store_true', default=False)
    parser.add_argument(
        '-V', '--version', action='store_true', help='Show version')

    subparsers = parser.add_subparsers()

    # BACKUP
    p = subparsers.add_parser(
        'backup',
        help="Perform a backup.")
    p.add_argument(
        'source',
        help='Source (url-like, e.g. file:///dev/sda or rbd://pool/imagename@snapshot)')
    p.add_argument(
        'name',
        help='Backup name')
    p.add_argument('-r', '--rbd', default=None, help='Hints as rbd json format')
    p.add_argument('-f', '--from-version', default=None, help='Use this version-uid as base')
    p.set_defaults(func='backup')

    # RESTORE
    p = subparsers.add_parser(
        'restore',
        help="Restore a given backup to a given target.")
    p.add_argument('-s', '--sparse', action='store_true', help='Faster. Restore '
        'only existing blocks (works only with file- and rbd-restore, not with lvm)')
    p.add_argument('-f', '--force', action='store_true', help='Force overwrite of existing files/devices/images')
    p.add_argument('version_uid')
    p.add_argument('target',
        help='Source (url-like, e.g. file:///dev/sda or rbd://pool/imagename)')
    p.set_defaults(func='restore')

    # RM
    p = subparsers.add_parser(
        'rm',
        help="Remove a given backup version. This will only remove meta data and you will have to cleanup after this.")
    p.add_argument('-f', '--force', action='store_true', help="Force removal of version, even if it's younger than the configured disallow_rm_when_younger_than_days.")
    p.add_argument('version_uid')
    p.set_defaults(func='rm')

    # SCRUB
    p = subparsers.add_parser(
        'scrub',
        help="Scrub a given backup and check for consistency.")
    p.add_argument('-s', '--source', default=None,
        help="Source, optional. If given, check if source matches backup in addition to checksum tests. url-like format as in backup.")
    p.add_argument('-p', '--percentile', default=100,
        help="Only check PERCENTILE percent of the blocks (value 0..100). Default: 100")
    p.add_argument('version_uid')
    p.set_defaults(func='scrub')

    # Export
    p = subparsers.add_parser(
        'export',
        help="Export the metadata of a backup uid into a file.")
    p.add_argument('version_uid')
    p.add_argument('filename', help="Export into this filename ('-' is for stdout)")
    p.set_defaults(func='export')

    # Import
    p = subparsers.add_parser(
        'import',
        help="Import the metadata of a backup from a file.")
    p.add_argument('filename', help="Read from this file ('-' is for stdin)")
    p.set_defaults(func='import_')

    # CLEANUP
    p = subparsers.add_parser(
        'cleanup',
        help="Clean unreferenced blobs.")
    p.add_argument(
        '-f', '--full', action='store_true', default=False,
        help='Do a full cleanup. This will read the full metadata from the data backend (i.e. backup storage) '
             'and compare it to the metadata in the meta backend. Unused data will then be deleted. '
             'This is a slow, but complete process. A full cleanup must not be run parallel to ANY other backy '
             'jobs.')
    p.add_argument(
        '-p', '--prefix', default=None,
        help='If you perform a full cleanup, you may add --prefix to only cleanup block uids starting '
             'with this prefix. This is for iterative cleanups. Example: '
             'cleanup --full --prefix=a')
    p.set_defaults(func='cleanup')

    # LS
    p = subparsers.add_parser(
        'ls',
        help="List existing backups.")
    p.add_argument('version_uid', nargs='?', default=None, help='Show verbose blocks for this version')
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
    p.add_argument('version_uid', nargs='?', default=None, help='Start an nbd server for this version')
    p.add_argument('-a', '--bind-address', default='127.0.0.1',
            help="Bind to this ip address (default: 127.0.0.1)")
    p.add_argument('-p', '--bind-port', default=10809,
            help="Bind to this port (default: 10809)")
    p.add_argument(
        '-r', '--read-only', action='store_true', default=False,
        help='Read only if set, otherwise a copy on write backup is created.')
    p.set_defaults(func='nbd')

    args = parser.parse_args()

    if args.version:
        print(__version__)
        sys.exit(0)

    if not hasattr(args, 'func'):
        parser.print_usage()
        sys.exit(1)

    if args.verbose:
        console_level = logging.DEBUG
    #elif args.func == 'scheduler':
        #console_level = logging.INFO
    else:
        console_level = logging.INFO

    Config = partial(_Config, conf_name='backy')
    config = Config(section='DEFAULTS')
    init_logging(config.get('logfile'), console_level)

    commands = Commands(args.machine_output, Config)
    func = getattr(commands, args.func)

    # Pass over to function
    func_args = dict(args._get_kwargs())
    del func_args['func']
    del func_args['verbose']
    del func_args['version']
    del func_args['machine_output']

    try:
        logger.debug('backup.{0}(**{1!r})'.format(args.func, func_args))
        func(**func_args)
        logger.info('Backy complete.\n')
        sys.exit(0)
    except Exception as e:
        logger.error('Unexpected exception')
        logger.exception(e)
        logger.info('Backy failed.\n')
        sys.exit(100)


if __name__ == '__main__':
    main()
