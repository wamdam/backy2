#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import datetime
import json
import os
import platform
import sqlite3
import time
import uuid
from binascii import hexlify, unhexlify
from collections import namedtuple

import sqlalchemy
from sqlalchemy import Column, String, Integer, BigInteger, ForeignKey, LargeBinary, Boolean, inspect, event
from sqlalchemy import func, distinct, desc
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.declarative import declarative_base, DeclarativeMeta
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import DateTime, TypeDecorator

from backy2.exception import InputDataError, InternalError
from backy2.logging import logger
from backy2.meta_backends import MetaBackend as _MetaBackend


class VersionUid(TypeDecorator):

    impl = Integer

    def process_bind_param(self, value, dialect):
        if value is not None:
            return int(value[1:])
        else:
            return None

    def process_result_value(self, value, dialect):
        if value is not None:
            return 'V' + str(value).zfill(10)
        else:
            return None

class Checksum(TypeDecorator):

    impl = LargeBinary

    def process_bind_param(self, value, dialect):
        if value is not None:
            return unhexlify(value)
        else:
            return None

    def process_result_value(self, value, dialect):
        if value is not None:
            return hexlify(value).decode('ascii')
        else:
            return None

Base = declarative_base()

class Stats(Base):
    __tablename__ = 'stats'
    date = Column("date", DateTime , default=func.now(), nullable=False)
    # No foreign key references here, so that we can keep the stats around even when the version is deleted
    version_uid = Column(VersionUid, primary_key=True)
    version_name = Column(String, nullable=False)
    version_snapshot_name = Column(String, nullable=False)
    version_size = Column(BigInteger, nullable=False)
    version_block_size = Column(BigInteger, nullable=False)
    bytes_read = Column(BigInteger, nullable=False)
    blocks_read = Column(BigInteger, nullable=False)
    bytes_written = Column(BigInteger, nullable=False)
    blocks_written = Column(BigInteger, nullable=False)
    bytes_found_dedup = Column(BigInteger, nullable=False)
    blocks_found_dedup = Column(BigInteger, nullable=False)
    bytes_sparse = Column(BigInteger, nullable=False)
    blocks_sparse = Column(BigInteger, nullable=False)
    duration_seconds = Column(BigInteger, nullable=False)


class Version(Base):
    __tablename__ = 'versions'
    uid = Column(VersionUid, primary_key=True, nullable=False)
    date = Column("date", DateTime , default=func.now(), nullable=False)
    name = Column(String, nullable=False, default='')
    snapshot_name = Column(String, nullable=False, server_default='', default='')
    size = Column(BigInteger, nullable=False)
    block_size = Column(Integer, nullable=False)
    valid = Column(Boolean, nullable=False)
    protected = Column(Boolean, nullable=False)

    tags = sqlalchemy.orm.relationship(
        'Tag',
        backref='version',
        order_by='asc(Tag.name)',
        passive_deletes=True,
    )

    blocks = sqlalchemy.orm.relationship(
        'Block',
        backref='version',
        order_by='asc(Block.id)',
        passive_deletes=True,
    )

    def __repr__(self):
       return "<Version(uid='%s', name='%s', snapshot_name='%s', date='%s')>" % (
                            self.uid, self.name, self.snapshot_name, self.date)


class Tag(Base):
    __tablename__ = 'tags'
    version_uid = Column(VersionUid, ForeignKey('versions.uid', ondelete='CASCADE'), primary_key=True, nullable=False)
    name = Column(String, nullable=False, primary_key=True)

    def __repr__(self):
       return "<Tag(version_uid='%s', name='%s')>" % (
                            self.version_uid, self.name)

DereferencedBlock = namedtuple('Block', ['uid', 'version_uid', 'id', 'date', 'checksum', 'size', 'valid'])
class Block(Base):
    __tablename__ = 'blocks'
    uid = Column(String(32), nullable=True, index=True)
    version_uid = Column(VersionUid, ForeignKey('versions.uid', ondelete='CASCADE'), primary_key=True, nullable=False)
    id = Column(Integer, primary_key=True, nullable=False)
    date = Column("date", DateTime , default=func.now(), nullable=False)
    checksum = Column(Checksum(_MetaBackend.MAXIMUM_CHECKSUM_LENGTH), index=True, nullable=True)
    size = Column(Integer, nullable=True)
    valid = Column(Boolean, nullable=False)


    def deref(self):
        """ Dereference this to a namedtuple so that we can pass it around
        without any thread inconsistencies
        """
        return DereferencedBlock(
            uid=self.uid,
            version_uid=self.version_uid,
            id=self.id,
            date=self.date,
            checksum=self.checksum,
            size=self.size,
            valid=self.valid,
        )

    def __repr__(self):
       return "<Block(id='%s', uid='%s', version_uid='%s')>" % (
                            self.id, self.uid, self.version_uid)


def inttime():
    return int(time.time())


class DeletedBlock(Base):
    __tablename__ = 'deleted_blocks'
    id = Column(Integer, primary_key=True)
    uid = Column(String(32), nullable=True, index=True)
    size = Column(Integer, nullable=True)
    # we need a date in order to find only delete candidates that are older than 1 hour.
    time = Column(BigInteger, default=inttime, nullable=False)

    def __repr__(self):
       return "<DeletedBlock(id='%s', uid='%s')>" % (
                            self.id, self.uid)


class Lock(Base):
    __tablename__ = 'locks'
    host = Column(String, nullable=False, primary_key=True)
    process_id = Column(String, nullable=False, primary_key=True)
    lock_name = Column(String, nullable=False, primary_key=True)
    reason = Column(String, nullable=False)
    date = Column("date", DateTime , default=func.now(), nullable=False)

    def __repr__(self):
        return "<Lock(host='%s' process_id='%s' lock_name='%s')>" % (
            self.host, self.process_id, self.lock_name)

class MetaBackend(_MetaBackend):
    """ Stores meta data in an sql database """

    NAME = 'sql'

    FLUSH_EVERY_N_BLOCKS = 1000

    _locking = None

    def __init__(self, config):
        _MetaBackend.__init__(self)

        our_config = config.get('metaBackend.{}'.format(self.NAME), types=dict)
        self._engine = sqlalchemy.create_engine(config.get_from_dict(our_config, 'engine', types=str))

    def open(self, _migratedb=True):
        if _migratedb:
            try:
                self.migrate_db()
            #except sqlalchemy.exc.OperationalError:
            except:
                raise RuntimeError('Invalid database ({}). Maybe you need to run initdb first?'.format(self._engine.url))

        # SQLite 3 supports checking of foreign keys but it needs to be enabled explicitly!
        # See: http://docs.sqlalchemy.org/en/latest/dialects/sqlite.html#foreign-key-support
        @event.listens_for(Engine, "connect")
        def set_sqlite_pragma(dbapi_connection, connection_record):
            if isinstance(dbapi_connection, sqlite3.Connection):
                cursor = dbapi_connection.cursor()
                cursor.execute("PRAGMA foreign_keys=ON")
                cursor.close()

        Session = sessionmaker(bind=self._engine)
        self._session = Session()
        self._locking = MetaBackendLocking(self._session)
        self._flush_block_counter = 0
        return self

    def migrate_db(self):
        # FIXME: fix to use supplied config
        # migrate the db to the lastest version
        from alembic.config import Config
        from alembic import command
        alembic_cfg = Config(os.path.join(os.path.dirname(os.path.realpath(__file__)), "sql_migrations", "alembic.ini"))
        with self._engine.begin() as connection:
            alembic_cfg.attributes['connection'] = connection
            #command.upgrade(alembic_cfg, "head", sql=True)
            command.upgrade(alembic_cfg, "head")

    def initdb(self, _destroydb=False, _migratedb=True):
        # This is dangerous and is only used by the test suite to get a clean slate
        if _destroydb:
            Base.metadata.drop_all(self._engine)

        # this will create all tables. It will NOT delete any tables or data.
        # Instead, it will raise when something can't be created.
        # TODO: explicitly check if the database is empty
        Base.metadata.create_all(self._engine, checkfirst=False)  # checkfirst False will raise when it finds an existing table

        # FIXME: fix to use supplied config
        if _migratedb:
            from alembic.config import Config
            from alembic import command
            alembic_cfg = Config(os.path.join(os.path.dirname(os.path.realpath(__file__)), "sql_migrations", "alembic.ini"))
            with self._engine.begin() as connection:
                alembic_cfg.attributes['connection'] = connection
                # mark the version table, "stamping" it with the most recent rev:
                command.stamp(alembic_cfg, "head")

    def _commit(self):
        self._session.commit()

    def set_version(self, version_name, snapshot_name, size, block_size, valid=False, protected=False):
        version = Version(
            name=version_name,
            snapshot_name=snapshot_name,
            size=size,
            block_size=block_size,
            valid=valid,
            protected=protected,
            )
        self._session.add(version)
        self._session.commit()
        return version

    def set_stats(self, version_uid, version_name, version_snapshot_name, version_size, version_block_size, bytes_read,
                  blocks_read, bytes_written, blocks_written, bytes_found_dedup, blocks_found_dedup, bytes_sparse,
                  blocks_sparse, duration_seconds):
        stats = Stats(
            version_uid=version_uid,
            version_name=version_name,
            version_snapshot_name=version_snapshot_name,
            version_size=version_size,
            version_block_size=version_block_size,
            bytes_read=bytes_read,
            blocks_read=blocks_read,
            bytes_written=bytes_written,
            blocks_written=blocks_written,
            bytes_found_dedup=bytes_found_dedup,
            blocks_found_dedup=blocks_found_dedup,
            bytes_sparse=bytes_sparse,
            blocks_sparse=blocks_sparse,
            duration_seconds=duration_seconds,
            )
        self._session.add(stats)
        self._session.commit()

    def get_stats(self, version_uid=None, limit=None):
        """ gets the <limit> newest entries """
        if version_uid:
            if limit is not None and limit < 1:
                return []
            stats = self._session.query(Stats).filter_by(version_uid=version_uid).all()
            if stats is None:
                raise KeyError('Statistics for version {} not found.'.format(version_uid))
            return stats
        else:
            if limit == 0:
                return []
            _stats = self._session.query(Stats).order_by(desc(Stats.date))
            if limit:
                _stats = _stats.limit(limit)
            return reversed(_stats.all())

    def set_version_invalid(self, uid):
        version = self.get_version(uid)
        version.valid = False
        self._session.commit()
        logger.info('Marked version invalid (UID {})'.format(
            uid,
            ))

    def set_version_valid(self, uid):
        version = self.get_version(uid)
        version.valid = True
        self._session.commit()
        logger.debug('Marked version valid (UID {})'.format(
            uid,
            ))

    def get_version(self, uid):
        version = self._session.query(Version).filter_by(uid=uid).first()
        if version is None:
            raise KeyError('Version {} not found.'.format(uid))
        return version

    def protect_version(self, uid):
        version = self.get_version(uid)
        version.protected = True
        self._session.commit()
        logger.debug('Marked version protected (UID {})'.format(
            uid,
            ))

    def unprotect_version(self, uid):
        version = self.get_version(uid)
        version.protected = False
        self._session.commit()
        logger.debug('Marked version unprotected (UID {})'.format(
            uid,
            ))

    def get_versions(self):
        return self._session.query(Version).order_by(Version.name, Version.date).all()

    def add_tag(self, version_uid, name):
        """ Add a tag to a version_uid, do nothing if the tag already exists.
        """
        tag = Tag(
            version_uid=version_uid,
            name=name,
            )
        self._session.add(tag)

    def remove_tag(self, version_uid, name):
        self._session.query(Tag).filter_by(version_uid=version_uid, name=name).delete()
        self._session.commit()

    def set_block(self, id, version_uid, block_uid, checksum, size, valid, _commit=True, _upsert=True):
        """ Upsert a block (or insert only when _upsert is False - this is only
        a performance improvement)
        """
        block = None
        if _upsert:
            block = self._session.query(Block).filter_by(id=id, version_uid=version_uid).first()

        if block:
            block.uid = block_uid
            block.checksum = checksum
            block.size = size
            block.valid = valid
            block.date = datetime.datetime.now()
        else:
            block = Block(
                id=id,
                version_uid=version_uid,
                uid=block_uid,
                checksum=checksum,
                size=size,
                valid=valid
                )
            self._session.add(block)
        self._flush_block_counter += 1
        if self._flush_block_counter % self.FLUSH_EVERY_N_BLOCKS == 0:
            t1 = time.time()
            self._session.flush()  # saves some ram
            t2 = time.time()
            logger.debug('Flushed meta backend in {:.2f}s'.format(t2-t1))
        if _commit:
            self._session.commit()

    def set_blocks_invalid(self, block_uid, checksum):
        _affected_version_uids = self._session.query(distinct(Block.version_uid)).filter_by(uid=block_uid, checksum=checksum).all()
        affected_version_uids = [v[0] for v in _affected_version_uids]
        self._session.query(Block).filter_by(uid=block_uid, checksum=checksum).update({'valid': False}, synchronize_session='fetch')
        self._session.commit()
        logger.info('Marked block invalid (UID {}, Checksum {}. Affected versions: {}'.format(
            block_uid,
            checksum,
            ', '.join(affected_version_uids)
            ))
        for version_uid in affected_version_uids:
            self.set_version_invalid(version_uid)
        return affected_version_uids

    def get_block(self, block_uid):
        return self._session.query(Block).filter_by(uid=block_uid).first()

    def get_block_by_checksum(self, checksum):
        return self._session.query(Block).filter_by(checksum=checksum, valid=True).first()

    def get_blocks_by_version(self, version_uid):
        return self._session.query(Block).filter_by(version_uid=version_uid).order_by(Block.id).all()

    def rm_version(self, version_uid):
        affected_blocks = self._session.query(Block).filter_by(version_uid=version_uid)
        num_blocks = affected_blocks.count()
        for affected_block in affected_blocks:
            if affected_block.uid:  # uid == None means sparse
                deleted_block = DeletedBlock(
                    uid=affected_block.uid,
                    size=affected_block.size,
                )
                self._session.add(deleted_block)
        affected_blocks.delete()
        # The following delete statement will cascade this delete to the blocks table,
        # but we've already moved the blocks to the deleted blocks table for later inspection.
        self._session.query(Version).filter_by(uid=version_uid).delete()
        self._session.commit()
        return num_blocks

    def get_delete_candidates(self, dt=3600):
        _stat_i = 0
        _stat_remove_from_delete_candidates = 0
        _stat_delete_candidates = 0
        while True:
            delete_candidates = self._session.query(
                DeletedBlock
            ).filter(
                DeletedBlock.time < (inttime() - dt)
            ).limit(250).all()  # http://stackoverflow.com/questions/7389759/memory-efficient-built-in-sqlalchemy-iterator-generator
            if not delete_candidates:
                break

            _remove_from_delete_candidate_uids = set()
            _delete_candidates = set()
            for candidate in delete_candidates:
                _stat_i += 1
                if _stat_i%1000 == 0:
                    logger.info("Cleanup-fast: {} false positives, {} data deletions.".format(
                        _stat_remove_from_delete_candidates,
                        _stat_delete_candidates,
                        ))

                block = self._session.query(
                    Block
                ).filter(
                    Block.uid == candidate.uid
                ).limit(1).scalar()
                if block:
                    _remove_from_delete_candidate_uids.add(candidate.uid)
                    _stat_remove_from_delete_candidates += 1
                else:
                    _delete_candidates.add(candidate.uid)
                    _stat_delete_candidates += 1

            if _remove_from_delete_candidate_uids:
                logger.debug("Cleanup-fast: Removing {} false positive delete candidates".format(len(_remove_from_delete_candidate_uids)))
                self._session.query(
                    DeletedBlock
                ).filter(
                    DeletedBlock.uid.in_(_remove_from_delete_candidate_uids)
                ).delete(synchronize_session=False)

            if _delete_candidates:
                logger.debug("Cleanup-fast: Sending {} delete candidates for final deletion".format(len(_delete_candidates)))
                self._session.query(
                    DeletedBlock
                ).filter(
                    DeletedBlock.uid.in_(_delete_candidates)
                ).delete(synchronize_session=False)
                yield(_delete_candidates)

        logger.info("Cleanup-fast: Cleanup finished. {} false positives, {} data deletions.".format(
            _stat_remove_from_delete_candidates,
            _stat_delete_candidates,
            ))

    def get_all_block_uids(self, prefix=None):
        if prefix:
            rows = self._session.query(distinct(Block.uid)).filter(Block.uid.like('{}%'.format(prefix))).all()
        else:
            rows = self._session.query(distinct(Block.uid)).all()
        return [b[0] for b in rows]

    # Based on: https://stackoverflow.com/questions/5022066/how-to-serialize-sqlalchemy-result-to-json/7032311,
    # https://stackoverflow.com/questions/1958219/convert-sqlalchemy-row-object-to-python-dict
    @staticmethod
    def new_backy2_encoder(ignore_fields, ignore_relationships):
        class Backy2Encoder(json.JSONEncoder):
            def default(self, obj):
                if isinstance(obj.__class__, DeclarativeMeta):
                    fields = {}

                    for field in inspect(obj).mapper.column_attrs:
                        ignore = False
                        for types, names in ignore_fields:
                            if isinstance(obj, types) and field.key in names:
                                ignore = True
                                break
                        if not ignore:
                            fields[field.key] = getattr(obj, field.key)

                    for relationship in inspect(obj).mapper.relationships:
                        ignore = False
                        for types, names in ignore_relationships:
                            if isinstance(obj, types) and relationship.key in names:
                                ignore = True
                                break
                        if not ignore:
                            fields[relationship.key] = getattr(obj, relationship.key)

                    return fields

                if isinstance(obj, datetime.datetime):
                    return obj.isoformat(timespec='seconds')

                return super().default(obj)
        return Backy2Encoder

    def export_any(self, root_key, root_value, f, ignore_fields=None, ignore_relationships=None):
        ignore_fields = list(ignore_fields) if ignore_fields is not None else []
        ignore_relationships = list(ignore_relationships) if ignore_relationships is not None else []

        # These are always ignored because they'd lead to a circle
        ignore_fields.append(((Tag, Block), ('version_uid',)))
        ignore_relationships.append(((Tag, Block), ('version',)))

        json.dump({'metadataVersion': self.METADATA_VERSION,
                   root_key: root_value },
                  f,
                  cls=self.new_backy2_encoder(ignore_fields, ignore_relationships),
                  check_circular=True,
                  indent=2,
                  )

    def export(self, version_uids, f):
        self.export_any('versions',
                        [self.get_version(version_uid) for version_uid in version_uids],
                        f
                        )

    def import_(self, f):
        try:
            f.seek(0)
            json_input = json.load(f)
        except Exception as exception:
            raise InputDataError('Import file is invalid.') from exception
        if json_input is None:
            raise InputDataError('Import file is empty.')
        if 'metadataVersion' not in json_input:
            raise InputDataError('Wrong import format.')
        if json_input['metadataVersion'] == '1.0.0':
            self.import_1_0_0(json_input)
        else:
            raise InputDataError('Wrong import format version {}.'.format(json_input['metadataVersion']))

    def import_1_0_0(self, json_input):
        for version_dict in json_input['versions']:
            try:
                self.get_version(version_dict['uid'])
            except KeyError:
                pass  # does not exist
            else:
                raise FileExistsError('Version {} already exists and cannot be imported.'.format(version_dict['uid']))

            version = Version(
                uid=version_dict['uid'],
                date=datetime.datetime.strptime(version_dict['date'], '%Y-%m-%dT%H:%M:%S'),
                name=version_dict['name'],
                snapshot_name=version_dict['snapshot_name'],
                size=version_dict['size'],
                block_size=version_dict['block_size'],
                valid=version_dict['valid'],
                protected=version_dict['protected'],
                )
            self._session.add(version)
            self._session.flush()

            for block_dict in version_dict['blocks']:
                block_dict['version_uid'] = version.uid
                block_dict['date'] = datetime.datetime.strptime(block_dict['date'], '%Y-%m-%dT%H:%M:%S')
            self._session.bulk_insert_mappings(Block, version_dict['blocks'])

            for tag_dict in version_dict['tags']:
                tag_dict['version_uid'] = version.uid
            self._session.bulk_insert_mappings(Tag, version_dict['tags'])

            self._session.commit()

    def locking(self):
        return self._locking

    def close(self):
        self._session.commit()
        self._locking.unlock_all()
        self._locking = None
        self._session.close()

class MetaBackendLocking:

    GLOBAL_LOCK = 'global'

    def __init__(self, session):
        self._session = session
        self._host = platform.node()
        self._uuid = uuid.uuid1().hex
        self._locks = {}

    def lock(self, lock_name=GLOBAL_LOCK, reason=None):
        if lock_name in self._locks:
            raise InternalError('Attempt to acquire lock "{}" twice'.format(lock_name))

        lock = Lock(
            host=self._host,
            process_id=self._uuid,
            lock_name=lock_name,
            reason=reason,
        )
        try:
            self._session.add(lock)
            self._session.commit()
        except SQLAlchemyError:
            self._session.rollback()
            return False
        except Exception:
            raise
        else:
            self._locks[lock_name] = lock
            return True

    def is_locked(self, lock_name=GLOBAL_LOCK):
        locks = self._session.query(Lock).filter_by(host=self._host, lock_name=lock_name, process_id=self._uuid).all()
        return len(locks) > 0

    def unlock(self, lock_name=GLOBAL_LOCK):
        if lock_name not in self._locks:
            raise InternalError('Attempt to release lock "{}" even though it isn\'t held'.format(lock_name))
        lock = self._locks[lock_name]
        self._session.delete(lock)
        self._session.commit()
        del self._locks[lock_name]

    def unlock_all(self):
        for lock_name, lock in self._locks.items():
            logger.error('Lock {} not released correctly, releasing it now.'.format(lock))
            self._session.delete(lock)
            self._session.commit()
        self._locks = {}
