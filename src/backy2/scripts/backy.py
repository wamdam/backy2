#!/usr/bin/env python
# -*- encoding: utf-8 -*-

import argparse
import fileinput
import logging
import os
import sys
from io import StringIO

import pkg_resources
from prettytable import PrettyTable

import backy2.exception
from backy2.config import Config
from backy2.logging import logger, init_logging
from backy2.meta_backends.sql import Version
from backy2.utils import hints_from_rbd_diff, backy_from_config, parametrized_hash_function

__version__ = pkg_resources.get_distribution('backy2').version


class Commands:
    """Proxy between CLI calls and actual backup code."""

    def __init__(self, machine_output, config):
        self.machine_output = machine_output
        self.config = config
        self.backy = backy_from_config(config)

    def backup(self, name, snapshot_name, source, rbd, from_version, tag=None):
        backy = self.backy()
        try:
            hints = None
            if rbd:
                data = ''.join([line for line in fileinput.input(rbd).readline()])
                hints = hints_from_rbd_diff(data)
            backy.backup(name, snapshot_name, source, hints, from_version, tag)
        except Exception:
            raise
        finally:
            backy.close()

    def restore(self, version_uid, target, sparse, force):
        backy = self.backy()
        try:
            backy.restore(version_uid, target, sparse, force)
        except Exception:
            raise
        finally:
            backy.close()

    def protect(self, version_uid):
        backy = self.backy()
        try:
            backy.protect(version_uid)
        except backy2.exception.NoChange:
            logger.warn('Version {} already was protected.'.format(version_uid))
        except Exception:
            raise
        finally:
            backy.close()

    def unprotect(self, version_uid):
        backy = self.backy()
        try:
            backy.unprotect(version_uid)
        except backy2.exception.NoChange:
            logger.warn('Version {} already was unprotected.'.format(version_uid))
        finally:
            backy.close()

    def rm(self, version_uids, force):
        disallow_rm_when_younger_than_days = self.config.get('disallowRemoveWhenYounger', types=int)
        backy = self.backy()
        try:
            for version_uid in version_uids:
                backy.rm(version_uid, force, disallow_rm_when_younger_than_days)
        except Exception:
            raise
        finally:
            backy.close()

    def scrub(self, version_uid, source, percentile):
        if percentile:
            percentile = int(percentile)
        backy = self.backy()
        try:
            backy.scrub(version_uid, source, percentile)
        except Exception:
            raise
        finally:
            backy.close()

    @staticmethod
    def _ls_versions_tbl_output(versions):
        tbl = PrettyTable()
        # TODO: number of invalid blocks, used disk space, shared disk space
        tbl.field_names = ['date', 'name', 'snapshot_name', 'size', 'block_size', 'uid',
                           'valid', 'protected', 'tags']
        tbl.align['name'] = 'l'
        tbl.align['snapshot_name'] = 'l'
        tbl.align['tags'] = 'l'
        tbl.align['size'] = 'r'
        tbl.align['block_size'] = 'r'
        for version in versions:
            tbl.add_row([
                version.date,
                version.name,
                version.snapshot_name,
                version.size,
                version.block_size,
                version.uid,
                version.valid,
                version.protected,
                ",".join(sorted([t.name for t in version.tags])),
                ])
        print(tbl)

    @staticmethod
    def _stats_tbl_output(stats):
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
                stat.date,
                stat.version_uid,
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
                stat.duration_seconds,
                ])
        print(tbl)

    def ls(self, name, snapshot_name, tag, include_blocks):
        backy = self.backy()
        try:
            versions = backy.ls()

            if name:
                versions = [v for v in versions if v.name == name]
            if snapshot_name:
                versions = [v for v in versions if v.snapshot_name == snapshot_name]
            if tag:
                versions = [v for v in versions if tag in [t.name for t in v.tags]]

            if self.machine_output:
                backy.meta_backend.export_any('versions',
                                              versions,
                                              sys.stdout,
                                              ignore_relationships=[((Version,), ('blocks',))] if not include_blocks else [],
                                              )
            else:
                self._ls_versions_tbl_output(versions)
        except Exception:
            raise
        finally:
            backy.close()

    def diff_meta(self, version_uid1, version_uid2):
        """ Output difference between two version in blocks.
        """
        # TODO: Feel free to create a default diff format.
        backy = self.backy()
        try:
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
        except Exception:
            raise
        finally:
            backy.close()

    def stats(self, version_uid, limit=None):
        if limit:
            limit = int(limit)
        backy = self.backy()
        try:
            stats = backy.stats(version_uid, limit)

            if self.machine_output:
                stats = list(stats) # resolve iterator, otherwise it's not serializable
                backy.meta_backend.export_any('stats',
                                              stats,
                                              sys.stdout,
                                              )
            else:
                self._stats_tbl_output(stats)
        except Exception:
            raise
        finally:
            backy.close()

    def cleanup(self, full, prefix=None):
        backy = self.backy()
        try:
            if full:
                backy.cleanup_full(prefix)
            else:
                backy.cleanup_fast()
        except Exception:
            raise
        finally:
            backy.close()

    def export(self, version_uid, filename='-'):
        backy = self.backy()
        try:
            if filename == '-':
                with StringIO() as f:
                    backy.export([version_uid], f)
                    print(f.getvalue())
            else:
                with open(filename, 'w') as f:
                    backy.export([version_uid], f)
        except Exception:
            raise
        finally:
            backy.close()

    def nbd(self, version_uid, bind_address, bind_port, read_only):
        from backy2.nbd.nbdserver import Server as NbdServer
        from backy2.nbd.nbd import BackyStore
        backy = self.backy()
        try:
            hash_function = parametrized_hash_function(self.config.get('hashFunction', types=str))
            cache_dir = self.config.get('nbd.cacheDirectory', types=str)
            store = BackyStore(backy, cachedir=cache_dir, hash_function=hash_function)
            addr = (bind_address, bind_port)
            server = NbdServer(addr, store, read_only)
            logger.info("Starting to serve nbd on %s:%s" % (addr[0], addr[1]))
            logger.info("You may now start")
            logger.info("  nbd-client -l %s -p %s" % (addr[0], addr[1]))
            logger.info("and then get the backup via")
            logger.info("  modprobe nbd")
            logger.info("  nbd-client -N <version> %s -p %s /dev/nbd0" % (addr[0], addr[1]))
            server.serve_forever()
        except Exception:
            raise
        finally:
            backy.close()

    def import_(self, filename='-'):
        backy = self.backy()
        try:
            if filename=='-':
                backy.import_(sys.stdin)
            else:
                with open(filename, 'r') as f:
                    backy.import_(f)
        except Exception:
            raise
        finally:
            backy.close()

    def add_tag(self, version_uid, name):
        try:
            backy = self.backy()
            backy.add_tag(version_uid, name)
        except backy2.exception.NoChange:
            logger.warn('Version {} already tagged with {}.'.format(version_uid, name))
        except Exception:
            raise
        finally:
            backy.close()

    def remove_tag(self, version_uid, name):
        backy = self.backy()
        try:
            backy.remove_tag(version_uid, name)
        except backy2.exception.NoChange:
            logger.warn('Version {} has no tag {}.'.format(version_uid, name))
        except Exception:
            raise
        finally:
            backy.close()

    def initdb(self):
        self.backy(initdb=True)


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
    p.add_argument('-s', '--snapshot-name', default='', help='Snapshot name (e.g. the name of the rbd snapshot)')
    p.add_argument('-r', '--rbd', default=None, help='Hints as rbd json format')
    p.add_argument('-f', '--from-version', default=None, help='Use this version-uid as base')
    p.add_argument(
        '-t', '--tag', action='append',  dest='tag', default=None,
        help='Use a specific tag for the target backup version-uid')
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

    # PROTECT
    p = subparsers.add_parser(
        'protect',
        help="Protect a backup version. Protected versions cannot be removed.")
    p.add_argument('version_uid')
    p.set_defaults(func='protect')

    # UNPROTECT
    p = subparsers.add_parser(
        'unprotect',
        help="Unprotect a backup version. Unprotected versions can be removed.")
    p.add_argument('version_uid')
    p.set_defaults(func='unprotect')

    # RM
    p = subparsers.add_parser(
        'rm',
        help="Remove the given backup versions. This will only remove meta data and you will have to cleanup after this.")
    p.add_argument('-f', '--force', action='store_true', help="Force removal of version, even if it's younger than the configured disallow_rm_when_younger_than_days.")
    p.add_argument('version_uids', metavar='version_uid', nargs='+')
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
    p.add_argument('version_uid', help="Version UID. Can be given multiple times.")
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
    p.add_argument('version_uid', nargs='?', default=None, help='Start an nbd server for this version')
    p.add_argument('-a', '--bind-address', default='127.0.0.1',
            help="Bind to this ip address (default: 127.0.0.1)")
    p.add_argument('-p', '--bind-port', default=10809,
            help="Bind to this port (default: 10809)")
    p.add_argument(
        '-r', '--read-only', action='store_true',
        help='Read only if set, otherwise a copy on write backup is created.')
    p.set_defaults(func='nbd')

    # ADD TAG
    p = subparsers.add_parser(
        'add-tag',
        help="Add a named tag to a backup version.")
    p.add_argument('version_uid')
    p.add_argument('name')
    p.set_defaults(func='add_tag')

    # REMOVE TAG
    p = subparsers.add_parser(
        'remove-tag',
        help="Remove a named tag from a backup version.")
    p.add_argument('version_uid')
    p.add_argument('name')
    p.set_defaults(func='remove_tag')


    args = parser.parse_args()

    if args.version:
        print(__version__)
        exit(0)

    if not hasattr(args, 'func'):
        parser.print_usage()
        exit(1)

    if args.verbose:
        console_level = logging.DEBUG
    #elif args.func == 'scheduler':
        #console_level = logging.INFO
    else:
        console_level = logging.INFO

    if args.configfile is not None and args.configfile != '':
        try:
            cfg = open(args.configfile, 'r', encoding='utf-8').read()
        except FileNotFoundError:
            logger.error('File {} not found.'.format(args.configfile))
            exit(1)
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
        {'exception': backy2.exception.UsageError, 'msg': 'Usage error', 'exit_code': os.EX_USAGE},
        {'exception': backy2.exception.AlreadyLocked, 'msg': 'Already locked error', 'exit_code': os.EX_NOPERM},
        {'exception': backy2.exception.InternalError, 'msg': 'Internal error', 'exit_code': os.EX_SOFTWARE},
        {'exception': backy2.exception.ConfigurationError, 'msg': 'Configuration error', 'exit_code': os.EX_CONFIG},
        {'exception': backy2.exception.InputDataError, 'msg': 'Input data error', 'exit_code': os.EX_DATAERR},
        {'exception': PermissionError, 'msg': 'Already locked error', 'exit_code': os.EX_NOPERM},
        {'exception': FileExistsError, 'msg': 'Already exists', 'exit_code': os.EX_CANTCREAT},
        {'exception': FileNotFoundError, 'msg': 'Not found', 'exit_code': os.EX_NOINPUT},
        {'exception': EOFError, 'msg': 'I/O error', 'exit_code': os.EX_IOERR},
        {'exception': IOError, 'msg': 'I/O error', 'exit_code': os.EX_IOERR},
        {'exception': OSError, 'msg': 'Not found', 'exit_code': os.EX_OSERR},
        {'exception': ConnectionError, 'msg': 'I/O error', 'exit_code': os.EX_IOERR},
        {'exception': LookupError, 'msg': 'Not found', 'exit_code': os.EX_NOINPUT},
        {'exception': Exception, 'msg': 'Other exception', 'exit_code': os.EX_SOFTWARE},
    ]

    try:
        logger.debug('backup.{0}(**{1!r})'.format(args.func, func_args))
        func(**func_args)
        logger.info('Backy complete.\n')
        exit(0)
    except Exception as exception:
        for case in exit_code_list:
            if isinstance(exception, case['exception']):
                logger.exception(case['msg'])
                logger.info('Backy failed.\n')
                exit(case['exit_code'])

if __name__ == '__main__':
    main()
