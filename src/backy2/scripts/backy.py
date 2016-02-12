#!/usr/bin/env python
# -*- encoding: utf-8 -*-

from configparser import ConfigParser  # python 3.3
import os
import sys
from backy2.readers.file import FileReader
from backy2.readers.rbd import RBDReader
from backy2.logging import logger, init_logging
from backy2.data_backends.file import FileBackend
from backy2.data_backends.s3 import S3Backend
from backy2.meta_backends.sql import SQLBackend
from backy2.enterprise.nbd import BackyStore
from backy2.backy import Backy

from functools import partial
from io import StringIO
from prettytable import PrettyTable
import argparse
import fileinput
import hashlib
import json
import logging

import pkg_resources
__version__ = pkg_resources.get_distribution('backy2').version


BLOCK_SIZE = 1024*4096  # 4MB
HASH_FUNCTION = hashlib.sha512

CFG = {
    'DEFAULTS': {
        'logfile': './backy.log',
        },
    'MetaBackend': {
        'type': 'sql',
        'engine': 'sqlite:////tmp/backy.sqlite',
        },
    'DataBackend': {
        'type': 'files',
        'path': '.',
        'aws_access_key_id': '',
        'aws_secret_access_key': '',
        'host': '',
        'port': '',
        'is_secure': '',
        'bucket_name': '',
        'simultaneous_writes': '1',
        },
    'Reader': {
        'type': 'file',
        'simultaneous_reads': '1',
        'ceph_conffile': '',
        },
    'NBD': {
        'cachedir': '/tmp',
        },
    }


class ConfigException(Exception):
    pass

class Config(dict):
    def __init__(self, base_config, conffile=None):
        if conffile:
            config = ConfigParser()
            config.read(conffile)
            sections = config.sections()
            difference = set(sections).difference(base_config.keys())
            if difference:
                raise ConfigException('Unknown config section(s): {}'.format(', '.join(difference)))
            for section in sections:
                items = config.items(section)
                _cfg = base_config[section]
                for item in items:
                    if item[0] not in _cfg:
                        raise ConfigException('Unknown setting "{}" in section "{}".'.format(item[0], section))
                    _cfg[item[0]] = item[1]
        for key, value in base_config.items():
            self[key] = value


def hints_from_rbd_diff(rbd_diff):
    """ Return the required offset:length tuples from a rbd json diff
    """
    data = json.loads(rbd_diff)
    return [(l['offset'], l['length'], True if l['exists']=='true' else False) for l in data]


class Commands():
    """Proxy between CLI calls and actual backup code."""

    def __init__(self, machine_output, config):
        self.machine_output = machine_output
        self.config = config

        # configure meta backend
        if config['MetaBackend']['type'] == 'sql':
            engine = config['MetaBackend']['engine']
            meta_backend = SQLBackend(engine)
        else:
            raise NotImplementedError('MetaBackend type {} unsupported.'.format(config['MetaBackend']['type']))

        # configure file backend
        if config['DataBackend']['type'] == 'files':
            data_backend = FileBackend(
                    config['DataBackend']['path'],
                    simultaneous_writes=int(config['DataBackend']['simultaneous_writes']),
                    )
        elif config['DataBackend']['type'] == 's3':
            data_backend = S3Backend(
                    aws_access_key_id=config['DataBackend']['aws_access_key_id'],
                    aws_secret_access_key=config['DataBackend']['aws_secret_access_key'],
                    host=config['DataBackend']['host'],
                    port=int(config['DataBackend']['port']),
                    is_secure=True if config['DataBackend']['is_secure'] in ('True', 'true', '1') else False,
                    bucket_name=config['DataBackend']['bucket_name'],
                    simultaneous_writes=int(config['DataBackend']['simultaneous_writes']),
                    )

        if config['Reader']['type'] == 'file':
            reader = FileReader(
                    simultaneous_reads=int(config['Reader']['simultaneous_reads']),
                    block_size=BLOCK_SIZE,
                    hash_function=HASH_FUNCTION,
                    )
        elif config['Reader']['type'] == 'librbd':
            reader = RBDReader(
                    simultaneous_reads=int(config['Reader']['simultaneous_reads']),
                    ceph_conffile=config['Reader']['ceph_conffile'],
                    block_size=BLOCK_SIZE,
                    hash_function=HASH_FUNCTION,
                    )

        self.backy = partial(Backy,
                meta_backend=meta_backend,
                data_backend=data_backend,
                reader=reader,
                block_size=BLOCK_SIZE,
                hash_function=HASH_FUNCTION,
                )


    def backup(self, name, source, rbd, from_version):
        backy = self.backy()
        hints = None
        if rbd:
            data = ''.join([line for line in fileinput.input(rbd).readline()])
            hints = hints_from_rbd_diff(data)
        backy.backup(name, source, hints, from_version)
        backy.close()


    def restore(self, version_uid, target, sparse):
        backy = self.backy()
        backy.restore(version_uid, target, sparse)
        backy.close()


    def rm(self, version_uid):
        backy = self.backy()
        backy.rm(version_uid)
        backy.close()


    def scrub(self, version_uid, source, percentile):
        if percentile:
            percentile = int(percentile)
        backy = self.backy()
        state = backy.scrub(version_uid, source, percentile)
        backy.close()
        if not state:
            exit(1)


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


    def stats(self, version_uid):
        backy = self.backy()
        stats = backy.stats(version_uid)
        if self.machine_output:
            self._stats_machine_output(stats)
        else:
            self._stats_tbl_output(stats)
        backy.close()


    def cleanup(self):
        backy = self.backy()
        backy.cleanup()
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
        from .enterprise.nbdserver import Server as NbdServer
        backy = self.backy()
        store = BackyStore(backy, cachedir=self.config['NBD']['cachedir'], hash_function=HASH_FUNCTION)
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
            exit(1)
        except ValueError as e:
            logger.error(str(e))
            exit(2)
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
        help='Source file')
    p.add_argument(
        'name',
        help='Backup name')
    p.add_argument('-r', '--rbd', default=None, help='Hints as rbd json format')
    p.add_argument('-f', '--from-version', default=None, help='Use this version-uid as base')
    p.set_defaults(func='backup')

    # RESTORE
    p = subparsers.add_parser(
        'restore',
        help="Restore a given backup with level to a given target.")
    p.add_argument('-s', '--sparse', action='store_true', help='Write restore file sparse (does not work with legacy devices)')
    p.add_argument('version_uid')
    p.add_argument('target')
    p.set_defaults(func='restore')

    # RM
    p = subparsers.add_parser(
        'rm',
        help="Remove a given backup version. This will only remove meta data and you will have to cleanup after this.")
    p.add_argument('version_uid')
    p.set_defaults(func='rm')

    # SCRUB
    p = subparsers.add_parser(
        'scrub',
        help="Scrub a given backup and check for consistency.")
    p.add_argument('-s', '--source', default=None,
        help="Source, optional. If given, check if source matches backup in addition to checksum tests.")
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
    p.set_defaults(func='stats')

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
        exit(0)

    if not hasattr(args, 'func'):
        parser.print_usage()
        sys.exit(0)

    here = os.path.dirname(os.path.abspath(__file__))
    conffilename = 'backy.cfg'
    conffiles = [
        os.path.join('/etc', conffilename),
        os.path.join('/etc', 'backy', conffilename),
        conffilename,
        os.path.join('..', conffilename),
        os.path.join('..', '..', conffilename),
        os.path.join('..', '..', '..', conffilename),
        os.path.join(here, conffilename),
        os.path.join(here, '..', conffilename),
        os.path.join(here, '..', '..', conffilename),
        os.path.join(here, '..', '..', '..', conffilename),
        ]
    config = None

    for conffile in conffiles:
        if args.verbose:
            print("Looking for {}... ".format(conffile), end="")
        if os.path.exists(conffile):
            if args.verbose:
                print("Found.")
            config = Config(CFG, conffile)
            break
        else:
            if args.verbose:
                print("")
    if not config:
        logger.warn("Running without conffile. Consider adding one at /etc/backy.cfg")
        config = Config(CFG)

    if args.verbose:
        console_level = logging.DEBUG
    #elif args.func == 'scheduler':
        #console_level = logging.INFO
    else:
        console_level = logging.INFO
    init_logging(config['DEFAULTS']['logfile'], console_level)

    commands = Commands(args.machine_output, config)
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
        sys.exit(1)


if __name__ == '__main__':
    main()
