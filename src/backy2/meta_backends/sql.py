#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import datetime
import json
import time
from binascii import hexlify, unhexlify
from collections import namedtuple

import os
import sqlalchemy
from sqlalchemy import Column, String, Integer, BigInteger, ForeignKey, LargeBinary, Boolean, inspect
from sqlalchemy import func, distinct, desc
from sqlalchemy.ext.declarative import declarative_base, DeclarativeMeta
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import DateTime, TypeDecorator

from backy2.exception import InputDataError
from backy2.logging import logger
from backy2.meta_backends import MetaBackend as _MetaBackend


class VersionUid(TypeDecorator):

    impl = Integer

    def process_bind_param(self, value, dialect):
        return int(value[1:])

    def process_result_value(self, value, dialect):
        return 'V' + str(value).zfill(10)

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
        cascade='all, delete-orphan',  # i.e. delete when version is deleted
    )

    blocks = sqlalchemy.orm.relationship(
        'Block',
        backref='version',
        order_by='asc(Block.id)',
        cascade='',
        passive_deletes=True
    )

    def __repr__(self):
       return "<Version(uid='%s', name='%s', snapshot_name='%s', date='%s')>" % (
                            self.uid, self.name, self.snapshot_name, self.date)


class Tag(Base):
    __tablename__ = 'tags'
    version_uid = Column(VersionUid, ForeignKey('versions.uid'), primary_key=True, nullable=False)
    name = Column(String, nullable=False, primary_key=True)

    def __repr__(self):
       return "<Tag(version_uid='%s', name='%s')>" % (
                            self.version_uid, self.name)

DereferencedBlock = namedtuple('Block', ['uid', 'version_uid', 'id', 'date', 'checksum', 'size', 'valid'])
class Block(Base):
    __tablename__ = 'blocks'
    uid = Column(String(32), nullable=True, index=True)
    version_uid = Column(VersionUid, ForeignKey('versions.uid'), primary_key=True, nullable=False)
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


class MetaBackend(_MetaBackend):
    """ Stores meta data in an sql database """

    NAME = 'sql'

    FLUSH_EVERY_N_BLOCKS = 1000

    def __init__(self, config):
        _MetaBackend.__init__(self)

        our_config = config.get('metaBackend.{}'.format(self.NAME), types=dict)
        self.engine = sqlalchemy.create_engine(config.get_from_dict(our_config, 'engine', types=str))

    def open(self, _migratedb=True):
        if _migratedb:
            try:
                self.migrate_db()
            #except sqlalchemy.exc.OperationalError:
            except:
                raise RuntimeError('Invalid database ({}). Maybe you need to run initdb first?'.format(self.engine.url))

        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        self._flush_block_counter = 0
        return self


    def migrate_db(self):
        # FIXME: fix to use supplied config
        # migrate the db to the lastest version
        from alembic.config import Config
        from alembic import command
        alembic_cfg = Config(os.path.join(os.path.dirname(os.path.realpath(__file__)), "sql_migrations", "alembic.ini"))
        with self.engine.begin() as connection:
            alembic_cfg.attributes['connection'] = connection
            #command.upgrade(alembic_cfg, "head", sql=True)
            command.upgrade(alembic_cfg, "head")


    def initdb(self, _destroydb=False, _migratedb=True):
        # This is dangerous and is only used by the test suite to get a clean slate
        if _destroydb:
            Base.metadata.drop_all(self.engine)

        # this will create all tables. It will NOT delete any tables or data.
        # Instead, it will raise when something can't be created.
        # TODO: explicitly check if the database is empty
        Base.metadata.create_all(self.engine, checkfirst=False)  # checkfirst False will raise when it finds an existing table

        # FIXME: fix to use supplied config
        if _migratedb:
            from alembic.config import Config
            from alembic import command
            alembic_cfg = Config(os.path.join(os.path.dirname(os.path.realpath(__file__)), "sql_migrations", "alembic.ini"))
            with self.engine.begin() as connection:
                alembic_cfg.attributes['connection'] = connection
                # mark the version table, "stamping" it with the most recent rev:
                command.stamp(alembic_cfg, "head")


    def _commit(self):
        self.session.commit()


    def set_version(self, version_name, snapshot_name, size, block_size, valid=False, protected=False):
        version = Version(
            name=version_name,
            snapshot_name=snapshot_name,
            size=size,
            block_size=block_size,
            valid=valid,
            protected=protected,
            )
        self.session.add(version)
        self.session.commit()
        return version


    def set_stats(self, version_uid, version_name, version_size,
            version_block_size, bytes_read, blocks_read, bytes_written,
            blocks_written, bytes_found_dedup, blocks_found_dedup,
            bytes_sparse, blocks_sparse, duration_seconds):
        stats = Stats(
            version_uid=version_uid,
            version_name=version_name,
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
        self.session.add(stats)
        self.session.commit()


    def get_stats(self, version_uid=None, limit=None):
        """ gets the <limit> newest entries """
        if version_uid:
            if limit is not None and limit < 1:
                return []
            stats = self.session.query(Stats).filter_by(version_uid=version_uid).all()
            if stats is None:
                raise KeyError('Statistics for version {} not found.'.format(version_uid))
            return stats
        else:
            if limit == 0:
                return []
            _stats = self.session.query(Stats).order_by(desc(Stats.date))
            if limit:
                _stats = _stats.limit(limit)
            return reversed(_stats.all())


    def set_version_invalid(self, uid):
        version = self.get_version(uid)
        version.valid = False
        self.session.commit()
        logger.info('Marked version invalid (UID {})'.format(
            uid,
            ))


    def set_version_valid(self, uid):
        version = self.get_version(uid)
        version.valid = True
        self.session.commit()
        logger.debug('Marked version valid (UID {})'.format(
            uid,
            ))


    def get_version(self, uid):
        version = self.session.query(Version).filter_by(uid=uid).first()
        if version is None:
            raise KeyError('Version {} not found.'.format(uid))
        return version


    def protect_version(self, uid):
        version = self.get_version(uid)
        version.protected = True
        self.session.commit()
        logger.debug('Marked version protected (UID {})'.format(
            uid,
            ))


    def unprotect_version(self, uid):
        version = self.get_version(uid)
        version.protected = False
        self.session.commit()
        logger.debug('Marked version unprotected (UID {})'.format(
            uid,
            ))


    def get_versions(self):
        return self.session.query(Version).order_by(Version.name, Version.date).all()


    def add_tag(self, version_uid, name):
        """ Add a tag to a version_uid, do nothing if the tag already exists.
        """
        tag = Tag(
            version_uid=version_uid,
            name=name,
            )
        self.session.add(tag)


    def remove_tag(self, version_uid, name):
        self.session.query(Tag).filter_by(version_uid=version_uid, name=name).delete()
        self.session.commit()


    def set_block(self, id, version_uid, block_uid, checksum, size, valid, _commit=True, _upsert=True):
        """ Upsert a block (or insert only when _upsert is False - this is only
        a performance improvement)
        """
        block = None
        if _upsert:
            block = self.session.query(Block).filter_by(id=id, version_uid=version_uid).first()

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
            self.session.add(block)
        self._flush_block_counter += 1
        if self._flush_block_counter % self.FLUSH_EVERY_N_BLOCKS == 0:
            t1 = time.time()
            self.session.flush()  # saves some ram
            t2 = time.time()
            logger.debug('Flushed meta backend in {:.2f}s'.format(t2-t1))
        if _commit:
            self.session.commit()


    def set_blocks_invalid(self, block_uid, checksum):
        _affected_version_uids = self.session.query(distinct(Block.version_uid)).filter_by(uid=block_uid, checksum=checksum).all()
        affected_version_uids = [v[0] for v in _affected_version_uids]
        self.session.query(Block).filter_by(uid=block_uid, checksum=checksum).update({'valid': False}, synchronize_session='fetch')
        self.session.commit()
        logger.info('Marked block invalid (UID {}, Checksum {}. Affected versions: {}'.format(
            block_uid,
            checksum,
            ', '.join(affected_version_uids)
            ))
        for version_uid in affected_version_uids:
            self.set_version_invalid(version_uid)
        return affected_version_uids


    def get_block(self, block_uid):
        return self.session.query(Block).filter_by(uid=block_uid).first()


    def get_block_by_checksum(self, checksum):
        return self.session.query(Block).filter_by(checksum=checksum, valid=True).first()


    def get_blocks_by_version(self, version_uid):
        return self.session.query(Block).filter_by(version_uid=version_uid).order_by(Block.id).all()


    def rm_version(self, version_uid):
        affected_blocks = self.session.query(Block).filter_by(version_uid=version_uid)
        num_blocks = affected_blocks.count()
        for affected_block in affected_blocks:
            if affected_block.uid:  # uid == None means sparse
                deleted_block = DeletedBlock(
                    uid=affected_block.uid,
                    size=affected_block.size,
                )
                self.session.add(deleted_block)
        affected_blocks.delete()
        # TODO: This is a sqlalchemy stupidity. cascade only works if the version
        # is deleted via session.delete() which first loads all objects into
        # memory. A session.query().filter().delete does not work with cascade.
        # Please see http://stackoverflow.com/questions/5033547/sqlalchemy-cascade-delete/12801654#12801654
        # for reference.
        self.session.query(Tag).filter_by(version_uid=version_uid).delete()
        self.session.query(Version).filter_by(uid=version_uid).delete()
        self.session.commit()
        return num_blocks


    def get_delete_candidates(self, dt=3600):
        _stat_i = 0
        _stat_remove_from_delete_candidates = 0
        _stat_delete_candidates = 0
        while True:
            delete_candidates = self.session.query(
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

                block = self.session.query(
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
                self.session.query(
                    DeletedBlock
                ).filter(
                    DeletedBlock.uid.in_(_remove_from_delete_candidate_uids)
                ).delete(synchronize_session=False)

            if _delete_candidates:
                logger.debug("Cleanup-fast: Sending {} delete candidates for final deletion".format(len(_delete_candidates)))
                self.session.query(
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
            rows = self.session.query(distinct(Block.uid)).filter(Block.uid.like('{}%'.format(prefix))).all()
        else:
            rows = self.session.query(distinct(Block.uid)).all()
        return [b[0] for b in rows]

    # Based on: https://stackoverflow.com/questions/5022066/how-to-serialize-sqlalchemy-result-to-json/7032311,
    # https://stackoverflow.com/questions/1958219/convert-sqlalchemy-row-object-to-python-dict
    @staticmethod
    def new_backy2_encoder():
        class Backy2Encoder(json.JSONEncoder):
            def default(self, obj):
                if isinstance(obj.__class__, DeclarativeMeta):
                    fields = {}

                    for field in inspect(obj).mapper.column_attrs:
                        if isinstance(obj, (Tag, Block)) and field.key == 'version_uid':
                            continue
                        fields[field.key] = getattr(obj, field.key)

                    for relationship in inspect(obj).mapper.relationships:
                        if isinstance(obj, (Tag, Block)) and relationship.key == 'version':
                            continue
                        fields[relationship.key] = getattr(obj, relationship.key)

                    return fields

                if isinstance(obj, datetime.datetime):
                    return obj.isoformat(timespec='seconds')

                return super().default(obj)
        return Backy2Encoder

    def export(self, version_uids, f):
        json.dump({'metadataVersion': self.METADATA_VERSION,
                   'versions': [self.get_version(version_uid) for version_uid in version_uids] },
                  f,
                  cls=self.new_backy2_encoder(),
                  check_circular=True,
                  indent=2,
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
            self.session.add(version)
            # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            # SQLAlchemy Bug
            # https://stackoverflow.com/questions/10154343/is-sqlalchemy-saves-order-in-adding-objects-to-session
            #
            # """
            # Within the same class, the order is indeed determined by the order
            # that add was called. However, you may see different orderings in the
            # INSERTs between different classes. If you add object a of type A and
            # later add object b of type B, but a turns out to have a foreign key
            # to b, you'll see an INSERT for b before the INSERT for a.
            # """
            # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            self.session.commit()
            # and because of this bug we must also try/except here instead of
            # simply leaving this to the database's transaction handling.
            try:
                for block_dict in version_dict['blocks']:
                    block = Block(
                        uid=block_dict['uid'],
                        version_uid=version_dict['uid'],
                        id=block_dict['id'],
                        date=datetime.datetime.strptime(block_dict['date'], '%Y-%m-%dT%H:%M:%S'),
                        checksum=block_dict['checksum'],
                        size=block_dict['size'],
                        valid=block_dict['valid'],
                    )
                    self.session.add(block)
                for tag_dict in version_dict['tags']:
                    tag = Tag(
                        version_uid=version_dict['uid'],
                        name=tag_dict['name'],
                    )
                    self.session.add(tag)
            except:  # see above
                self.rm_version(version_dict['uid'])
            finally:
                self.session.commit()


    def close(self):
        self.session.commit()
        self.session.close()


