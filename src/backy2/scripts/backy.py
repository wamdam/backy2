#!/usr/bin/env python
# -*- encoding: utf-8 -*-

from backy2.config import Config as _Config
from backy2.logging import logger, init_logging
from backy2.utils import hints_from_rbd_diff, backy_from_config, convert_to_timedelta, parse_expire_date, humanize
from datetime import date, datetime
from functools import partial
from io import StringIO
from prettytable import PrettyTable
import argparse
import csv
import fileinput
import hashlib
import logging
import sys


import pkg_resources
__version__ = pkg_resources.get_distribution('backy2').version


class Commands():
    """Proxy between CLI calls and actual backup code."""

    def __init__(self, machine_output, skip_header, human_readable, Config):
        self.machine_output = machine_output
        self.skip_header = skip_header
        self.human_readable = human_readable
        self.Config = Config
        self.backy = backy_from_config(Config)


    def _tbl_output(self, fields, data, alignments=None, humanize_columns=None):
        """
        outputs data based on fields list.
        fields: list(fieldnames)
        data: list of dicts with keys containing field names
        alignments: dict(name: direction)
        humanize_columns: List of columns of file sizes
        """
        tbl = PrettyTable()
        tbl.field_names = fields
        if alignments:
            for key, value in alignments.items():
                tbl.align[key] = value
        for d in data:
            values = []
            for field_name in fields:
                if type(d[field_name]) is datetime:
                    values.append(d[field_name].strftime('%Y-%m-%d %H:%M:%S'))
                elif self.human_readable and humanize_columns and field_name in humanize_columns:
                    values.append(humanize(d[field_name]))
                else:
                    values.append(d[field_name])
            tbl.add_row(values)
        if self.skip_header:
            tbl.header = False
        print(tbl)


    def _machine_output(self, fields, data, humanize_columns=None):
        if not self.skip_header:
            print('|'.join(fields))
        for d in data:
            values = []
            for field_name in fields:
                if type(d[field_name]) is datetime:
                    values.append(d[field_name].strftime('%Y-%m-%d %H:%M:%S'))
                elif self.human_readable and humanize_columns and field_name in humanize_columns:
                    values.append(humanize(d[field_name]))
                else:
                    values.append(d[field_name])
            print('|'.join(map(str, values)))


    def backup(self, name, snapshot_name, source, rbd, from_version, tag=None, expire=None, continue_version=None):
        expire_date = None
        if expire:
            try:
                expire_date = parse_expire_date(expire)
            except ValueError as e:
                logger.error(str(e))
                exit(1)

        backy = self.backy()
        hints = None
        if rbd:
            data = ''.join([line for line in fileinput.input(rbd).readline()])
            hints = hints_from_rbd_diff(data)
        if tag:
            tags = [t.strip() for t in list(csv.reader(StringIO(tag)))[0]]
        else:
            tags = None
        version_uid = backy.backup(name, snapshot_name, source, hints, from_version, tags, expire_date, continue_version, output_version_uid_early=self.machine_output)
        if self.machine_output:
            print(version_uid)
        backy.close()


    def restore(self, version_uid, target, sparse, force, continue_from):
        backy = self.backy()
        backy.restore(version_uid, target, sparse, force, int(continue_from))
        backy.close()


    def protect(self, version_uid):
        backy = self.backy()
        backy.protect(version_uid)
        backy.close()


    def unprotect(self, version_uid):
        backy = self.backy()
        backy.unprotect(version_uid)
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


    def ls(self, name, snapshot_name, tag, expired, fields):
        backy = self.backy()
        versions = backy.ls()
        if name:
            versions = [v for v in versions if v.name == name]
        if snapshot_name:
            versions = [v for v in versions if v.snapshot_name == snapshot_name]
        if tag:
            versions = [v for v in versions if tag in [t.name for t in v.tags]]
        if expired:
            versions = [v for v in versions if v.expire and v.expire < datetime.utcnow()]

        fields = [f.strip() for f in list(csv.reader(StringIO(fields)))[0]]
        values = []
        for version in versions:
            values.append({
                'date': version.date,
                'name': version.name,
                'snapshot_name': version.snapshot_name,
                'size': version.size,
                'size_bytes': version.size_bytes,
                'uid': version.uid,
                'valid': int(version.valid),
                'protected': int(version.protected),
                'tags': ",".join([t.name for t in version.tags]),
                'expire': version.expire if version.expire else '',
            })
        if self.machine_output:
            self._machine_output(fields, values, humanize_columns=('size_bytes',))
        else:
            self._tbl_output(
                    fields,
                    values,
                    alignments={'name': 'l', 'snapshot_name': 'l', 'tags': 'l', 'size': 'r', 'size_bytes': 'r'},
                    humanize_columns=('size_bytes',),
                    )
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


    def du(self, version_uid=None, fields=None):
        """ Output disk usage for a version
        """
        backy = self.backy()
        if version_uid:
            version_uids = [version_uid]
        else:
            _versions = backy.ls()
            version_uids = [v.uid for v in _versions]

        fields = [f.strip() for f in list(csv.reader(StringIO(fields)))[0]]
        values = []
        for version_uid in version_uids:
            stats = backy.du(version_uid)
            values.append({
                'Real': stats['real_space'],
                'Null': stats['null_space'],
                'Dedup Own': stats['dedup_own'],
                'Dedup Others': stats['dedup_others'],
                'Individual': stats['nodedup'],
                'Est. Space': stats['backy_space'],
                'Est. Space freed': stats['space_freed'],
                })

        if self.machine_output:
            self._machine_output(fields, values, humanize_columns=('Real', 'Null', 'Dedup Own', 'Dedup Others', 'Individual', 'Est. Space', 'Est. Space freed'))
        else:
            self._tbl_output(fields, values, alignments={
                'Real': 'r',
                'Null': 'r',
                'Dedup Own': 'r',
                'Dedup Others': 'r',
                'Individual': 'r',
                'Est. Space': 'r',
                'Est. Space freed': 'r',
                }, humanize_columns=('Real', 'Null', 'Dedup Own', 'Dedup Others', 'Individual', 'Est. Space', 'Est. Space freed'),
            )


    def stats(self, version_uid, fields, limit=None):
        backy = self.backy()
        if limit is not None:
            limit = int(limit)
        stats = backy.stats(version_uid, limit)
        fields = [f.strip() for f in list(csv.reader(StringIO(fields)))[0]]

        values = []
        for stat in stats:
            values.append({
                'blocks dedup': stat.blocks_found_dedup,
                'blocks read': stat.blocks_read,
                'blocks sparse': stat.blocks_sparse,
                'blocks written': stat.blocks_written,
                'bytes dedup': stat.bytes_found_dedup,
                'bytes read': stat.bytes_read,
                'bytes sparse': stat.bytes_sparse,
                'bytes written': stat.bytes_written,
                'date': stat.date,
                'duration (s)': stat.duration_seconds,
                'metadata': stat.metadata,
                'name': stat.version_name,
                'size blocks': stat.version_size_blocks,
                'size bytes': stat.version_size_bytes,
                'uid': stat.version_uid,
            })
        if self.machine_output:
            self._machine_output(fields, values, humanize_columns=('size bytes', 'bytes read', 'bytes written', 'bytes dedup', 'bytes sparse'))
        else:
            self._tbl_output(fields, values, alignments={
                'name': 'l',
                'size bytes': 'r',
                'size blocks': 'r',
                'bytes read': 'r',
                'blocks read': 'r',
                'bytes written': 'r',
                'blocks written': 'r',
                'bytes dedup': 'r',
                'blocks dedup': 'r',
                'bytes sparse': 'r',
                'blocks sparse': 'r',
                'duration (s)': 'r',
                }, humanize_columns=('size bytes', 'bytes read', 'bytes written', 'bytes dedup', 'bytes sparse'),
            )
        backy.close()


    def cleanup(self, full, dangerous_force, prefix=None):
        backy = self.backy()
        if full:
            backy.cleanup_full(prefix)
        elif dangerous_force:
            backy.cleanup_fast(dt=0)
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


    def fuse(self, mount):
        backy = self.backy()
        backy.fuse(mount)


    def rekey(self, oldkey):
        backy = self.backy()
        backy.rekey(oldkey)


    def migrate_encryption(self, version_uid):
        backy = self.backy()
        backy.migrate_encryption(version_uid)


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


    def add_tag(self, version_uid, name):
        try:
            backy = self.backy()
            tags = [t.strip() for t in list(csv.reader(StringIO(name)))[0]]
            for tag in tags:
                backy.add_tag(version_uid, tag)
            backy.close()
        except:
            logger.warn('Unable to add tag.')


    def remove_tag(self, version_uid, name):
        backy = self.backy()
        tags = [t.strip() for t in list(csv.reader(StringIO(name)))[0]]
        for tag in tags:
            backy.remove_tag(version_uid, tag)
        backy.close()


    def expire(self, version_uid, expire):
        if not expire:  # empty string
            expire_date = None
        else:
            try:
                expire_date = parse_expire_date(expire)
            except ValueError as e:
                logger.error(str(e))
                exit(1)
        try:
            backy = self.backy()
            backy.expire_version(version_uid, expire_date)
            backy.close()
        except:
            logger.warn('Unable to expire version.')


    def due(self, name, schedulers, fields):
        schedulers = [s.strip() for s in list(csv.reader(StringIO(schedulers)))[0]]
        backy = self.backy()
        versions = backy.ls()
        if not name:
            names = set([v.name for v in versions])
        else:
            names = [name]

        due_backups = {}   # name: list of tags
        for name in names:
            _due_schedulers = set()
            _due_backup_expire_date = datetime.utcnow()
            _due_backup_due_since = datetime.utcnow()
            for scheduler in schedulers:
                interval = convert_to_timedelta(self.Config(section=scheduler).get('interval'))
                keep = self.Config(section=scheduler).getint('keep')
                sla = convert_to_timedelta(self.Config(section=scheduler).get('sla'))

                _due_backup = backy.get_due_backups(name, scheduler, interval, keep, sla)  # True/False
                if _due_backup:
                    _due_schedulers.add(scheduler)
                    _due_backup_expire_date = max(_due_backup_expire_date, datetime.utcnow() + (keep + 1) * interval)
                    _due_backup_due_since = min(_due_backup_due_since, _due_backup)
            if _due_schedulers:
                due_backups[name] = {
                    'schedulers': _due_schedulers,
                    'due_backup_expire_date': _due_backup_expire_date,
                    'due_backup_since': _due_backup_due_since,
                    }

        field_names = [f.strip() for f in list(csv.reader(StringIO(fields)))[0]]
        values = []
        for name, backup_info in due_backups.items():
            values.append({
                'name': name,
                'schedulers': ",".join(backup_info['schedulers']),
                'expire_date': backup_info['due_backup_expire_date'],
                'due_since': backup_info['due_backup_since'],
                })
            values.sort(key=lambda v: v['due_since'])
        if self.machine_output:
            self._machine_output(field_names, values)
        else:
            self._tbl_output(field_names, values, alignments={'name': 'l', 'schedulers': 'l'})


    def sla(self, name, schedulers, fields):
        schedulers = [s.strip() for s in list(csv.reader(StringIO(schedulers)))[0]]
        backy = self.backy()
        versions = backy.ls()
        if not name:
            names = set([v.name for v in versions])
        else:
            names = [name]

        sla_breaches = {}  # name: list of breaches
        for name in names:
            for scheduler in schedulers:
                interval = convert_to_timedelta(self.Config(section=scheduler).get('interval'))
                keep = self.Config(section=scheduler).getint('keep')
                sla = convert_to_timedelta(self.Config(section=scheduler).get('sla'))

                _sla_breaches = backy.get_sla_breaches(name, scheduler, interval, keep, sla)  # list of strings
                sla_breaches.setdefault(name, []).extend(_sla_breaches)

        field_names = [f.strip() for f in list(csv.reader(StringIO(fields)))[0]]
        values = []
        for name, breaches in sla_breaches.items():
            for breach in breaches:
                values.append({'name': name, 'breach': breach})
        if self.machine_output:
            self._machine_output(field_names, values)
        else:
            self._tbl_output(field_names, values, alignments={'name': 'l', 'breach': 'l'})


    def initdb(self):
        self.backy(initdb=True)



def main():
    parser = argparse.ArgumentParser(
        description='Backup and restore for block devices.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-v', '--verbose', action='store_true', help='verbose output')
    parser.add_argument(
        '-d', '--debug', action='store_true', help='debug output')
    parser.add_argument(
        '-m', '--machine-output', action='store_true', default=False)
    parser.add_argument(
        '-s', '--skip-header', action='store_true', default=False)
    parser.add_argument(
        '-r', '--human-readable', action='store_true', default=False)
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
    p.add_argument('-c', '--continue-version', default=None, help='Continue backup on this version-uid')
    p.add_argument(
        '-t', '--tag', default=None,
        help='Use a specific tag (or multiple comma-separated tags) for the target backup version-uid')
    p.add_argument('-e', '--expire', default='', help='Expiration date (yyyy-mm-dd or "yyyy-mm-dd HH-MM-SS") (optional)')
    p.set_defaults(func='backup')

    # RESTORE
    p = subparsers.add_parser(
        'restore',
        help="Restore a given backup to a given target.")
    p.add_argument('-s', '--sparse', action='store_true', help='Faster. Restore '
        'only existing blocks (works only with file- and rbd-restore, not with lvm)')
    p.add_argument('-f', '--force', action='store_true', help='Force overwrite of existing files/devices/images')
    p.add_argument('-c', '--continue-from', default=0, help='Continue from this block (only use this for partially failed restores!)')
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
    p.add_argument(
        '--dangerous-force', action='store_true', default=False,
        help='Seriously, do not use this outside of testing and development.')
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
    p.add_argument('-e', '--expired', action='store_true', default=False,
            help="Only list expired versions (expired < now)")
    p.add_argument('-f', '--fields', default="date,name,snapshot_name,size,size_bytes,uid,valid,protected,tags,expire",
            help="Show these fields (comma separated). Available: date,name,snapshot_name,size,size_bytes,uid,valid,protected,tags,expire")


    p.set_defaults(func='ls')

    # STATS
    p = subparsers.add_parser(
        'stats',
        help="Show statistics")
    p.add_argument('version_uid', nargs='?', default=None, help='Show statistics for this version')
    p.add_argument('-f', '--fields', default="date,uid,name,size bytes,size blocks,bytes read,blocks read,bytes written,blocks written,bytes dedup,blocks dedup,bytes sparse,blocks sparse,duration (s)",
            help="Show these fields (comma separated). Available: date,uid,name,size bytes,size blocks,bytes read,blocks read,bytes written,blocks written,bytes dedup,blocks dedup,bytes sparse,blocks sparse,duration (s)")
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

    # disk usage
    p = subparsers.add_parser(
        'du',
        help="Get disk usage for a version or for all versions")
    p.add_argument('version_uid', nargs='?', default=None, help='Show disk usage for this version')
    p.add_argument('-f', '--fields', default="Real,Null,Dedup Own,Dedup Others,Individual,Est. Space,Est. Space freed",
            help="Show these fields (comma separated). Available: Real,Null,Dedup Own,Dedup Others,Individual,Est. Space,Est. Space freed)")
    p.set_defaults(func='du')

    # FUSE
    p = subparsers.add_parser(
        'fuse',
        help="Fuse mount backy backups")
    p.add_argument('mount', help='Mountpoint')
    p.set_defaults(func='fuse')

    # Re-Keying
    p = subparsers.add_parser(
        'rekey',
        help="Re-Key all blocks in backy2 with a new key in the config. This will NOT encrypt unencrypted blocks or recrypt existing blocks.")
    p.add_argument('oldkey', help='The old key as it was found in the config')
    p.set_defaults(func='rekey')

    # Migrate encryption
    p = subparsers.add_parser(
        'migrate-encryption',
        help="Create a new version with blocks migrated/encrypted to the latest encryption version.")
    p.add_argument('version_uid', help='The version uid to migrate')
    p.set_defaults(func='migrate_encryption')

    # ADD TAG
    p = subparsers.add_parser(
        'add-tag',
        help="Add a named tag (or many comma-separated tags) to a backup version.")
    p.add_argument('version_uid')
    p.add_argument('name')
    p.set_defaults(func='add_tag')

    # REMOVE TAG
    p = subparsers.add_parser(
        'remove-tag',
        help="Remove a named tag (or many comma-separated tags) from a backup version.")
    p.add_argument('version_uid')
    p.add_argument('name')
    p.set_defaults(func='remove_tag')

    # EXPIRE
    p = subparsers.add_parser(
        'expire',
        help="""Set expiration date for a backup version. Date format is yyyy-mm-dd or "yyyy-mm-dd HH:MM:SS" (e.g. 2020-01-23). HINT: Create with 'date +"%%Y-%%m-%%d" -d "today + 7 days"'""")
    p.add_argument('version_uid')
    p.add_argument('expire')
    p.set_defaults(func='expire')

    # DUE
    p = subparsers.add_parser(
        'due',
        help="""Based on the schedulers in the config file, calculate the due backups including tags.""")
    p.add_argument('name', nargs='?', default=None, help='Show due backups for this version name (optional, if not given, show due backups for all names).')
    p.add_argument('-s', '--schedulers',default="daily,weekly,monthly",
            help="Use these schedulers as defined in backy.cfg (default: daily,weekly,monthly)")
    p.add_argument('-f', '--fields', default="name,schedulers,expire_date,due_since",
            help="Show these fields (comma separated). Available: name,schedulers,expire_date")
    p.set_defaults(func='due')

    # SLA
    p = subparsers.add_parser(
        'sla',
        help="""Based on the schedulers in the config file, calculate the information about SLA.""")
    p.add_argument('name', nargs='?', default=None, help='Show SLA breaches for this version name (optional, if not given, show SLA breaches for all names).')
    p.add_argument('-s', '--schedulers',default="daily,weekly,monthly",
            help="Use these schedulers as defined in backy.cfg (default: daily,weekly,monthly)")
    p.add_argument('-f', '--fields', default="name,breach",
            help="Show these fields (comma separated). Available: name,breach")
    p.set_defaults(func='sla')


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

    if args.debug:
        debug = True
    else:
        debug = False

    if args.configfile is not None and args.configfile != '':
        try:
            cfg = open(args.configfile, 'r', encoding='utf-8').read()
        except FileNotFoundError:
            logger.error('File not found: {}'.format(args.configfile))
            sys.exit(1)
        Config = partial(_Config, cfg=cfg)
    else:
        Config = partial(_Config, conf_name='backy')
    config = Config(section='DEFAULTS')

    # logging ERROR only when machine output is selected
    if args.machine_output:
        init_logging(config.get('logfile'), logging.ERROR, debug)
    else:
        init_logging(config.get('logfile'), console_level, debug)

    commands = Commands(args.machine_output, args.skip_header, args.human_readable, Config)
    func = getattr(commands, args.func)

    # Pass over to function
    func_args = dict(args._get_kwargs())
    del func_args['configfile']
    del func_args['func']
    del func_args['verbose']
    del func_args['debug']
    del func_args['version']
    del func_args['machine_output']
    del func_args['skip_header']
    del func_args['human_readable']

    try:
        logger.debug('backup.{0}(**{1!r})'.format(args.func, func_args))
        func(**func_args)
        logger.info('Backy complete.\n')
        sys.exit(0)
    except Exception as e:
        if args.debug:
            logger.error('Unexpected exception')
            logger.exception(e)
        else:
            logger.error(e)
        logger.error('Backy failed.\n')
        sys.exit(100)


if __name__ == '__main__':
    main()
