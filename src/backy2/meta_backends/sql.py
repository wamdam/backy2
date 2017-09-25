#!/usr/bin/env python
# -*- encoding: utf-8 -*-
from backy2.logging import logger
from backy2.meta_backends import MetaBackend as _MetaBackend
from collections import namedtuple
from sqlalchemy import Column, String, Integer, BigInteger, ForeignKey
from sqlalchemy import func, distinct, desc
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import DateTime
import csv
import datetime
import os
import sqlalchemy
import sys
import time
import uuid


METADATA_VERSION = '2.2'

DELETE_CANDIDATE_MAYBE = 0
DELETE_CANDIDATE_SURE = 1
DELETE_CANDIDATE_DELETED = 2


Base = declarative_base()

class Stats(Base):
    __tablename__ = 'stats'
    date = Column("date", DateTime , default=func.now(), nullable=False)
    version_uid = Column(String(36), primary_key=True)
    version_name = Column(String, nullable=False)
    version_size_bytes = Column(BigInteger, nullable=False)
    version_size_blocks = Column(BigInteger, nullable=False)
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
    uid = Column(String(36), primary_key=True)
    date = Column("date", DateTime , default=func.now(), nullable=False)
    name = Column(String, nullable=False, default='')
    snapshot_name = Column(String, nullable=False, server_default='', default='')
    size = Column(BigInteger, nullable=False)
    size_bytes = Column(BigInteger, nullable=False)
    valid = Column(Integer, nullable=False)
    protected = Column(Integer, nullable=False)
    tags = sqlalchemy.orm.relationship(
            "Tag",
            backref="version",
            cascade="all, delete, delete-orphan",  # i.e. delete when version is deleted
            )

    def __repr__(self):
       return "<Version(uid='%s', name='%s', snapshot_name='%s', date='%s')>" % (
                            self.uid, self.name, self.snapshot_name, self.date)


class Tag(Base):
    __tablename__ = 'tags'
    version_uid = Column(String(36), ForeignKey('versions.uid'), primary_key=True, nullable=False)
    name = Column(String, nullable=False, primary_key=True)

    def __repr__(self):
       return "<Tag(version_uid='%s', name='%s')>" % (
                            self.version_uid, self.name)


DereferencedBlock = namedtuple('Block', ['uid', 'version_uid', 'id', 'date', 'checksum', 'size', 'valid'])
class Block(Base):
    __tablename__ = 'blocks'
    uid = Column(String(32), nullable=True, index=True)
    version_uid = Column(String(36), ForeignKey('versions.uid'), primary_key=True, nullable=False)
    id = Column(Integer, primary_key=True, nullable=False)
    date = Column("date", DateTime , default=func.now(), nullable=False)
    checksum = Column(String(128), index=True, nullable=True)
    size = Column(BigInteger, nullable=True)
    valid = Column(Integer, nullable=False)


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
    size = Column(BigInteger, nullable=True)
    delete_candidate = Column(Integer, nullable=False)
    # we need a date in order to find only delete candidates that are older than 1 hour.
    time = Column(BigInteger, default=inttime, nullable=False)

    def __repr__(self):
       return "<DeletedBlock(id='%s', uid='%s')>" % (
                            self.id, self.uid)


class MetaBackend(_MetaBackend):
    """ Stores meta data in an sql database """

    FLUSH_EVERY_N_BLOCKS = 1000

    def __init__(self, config):
        _MetaBackend.__init__(self)
        # engine = sqlalchemy.create_engine(config.get('engine'), echo=True)
        self.engine = sqlalchemy.create_engine(config.get('engine'))


    def open(self):
        try:
            self.migrate_db(self.engine)
        #except sqlalchemy.exc.OperationalError:
        except:
            logger.error('Invalid database ({}). Please run initdb first.'.format(self.engine.url))
            sys.exit(1)  # TODO: Return something (or raise)
            #raise RuntimeError('Invalid database')

        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        self._flush_block_counter = 0
        return self


    def migrate_db(self, engine):
        # migrate the db to the lastest version
        from alembic.config import Config
        from alembic import command
        alembic_cfg = Config(os.path.join(os.path.dirname(os.path.realpath(__file__)), "sql_migrations", "alembic.ini"))
        with self.engine.begin() as connection:
            alembic_cfg.attributes['connection'] = connection
            #command.upgrade(alembic_cfg, "head", sql=True)
            command.upgrade(alembic_cfg, "head")


    def initdb(self):
        # this will create all tables. It will NOT delete any tables or data.
        # Instead, it will raise when something can't be created.
        # TODO: explicitly check if the database is empty
        Base.metadata.create_all(self.engine, checkfirst=False)  # checkfirst False will raise when it finds an existing table

        from alembic.config import Config
        from alembic import command
        alembic_cfg = Config(os.path.join(os.path.dirname(os.path.realpath(__file__)), "sql_migrations", "alembic.ini"))
        with self.engine.begin() as connection:
            alembic_cfg.attributes['connection'] = connection
            # mark the version table, "stamping" it with the most recent rev:
            command.stamp(alembic_cfg, "head")


    def _uid(self):
        return str(uuid.uuid1())


    def _commit(self):
        self.session.commit()


    def set_version(self, version_name, snapshot_name, size, size_bytes, valid, protected=0):
        uid = self._uid()
        version = Version(
            uid=uid,
            name=version_name,
            snapshot_name=snapshot_name,
            size=size,
            size_bytes=size_bytes,
            valid=valid,
            protected=protected,
            )
        self.session.add(version)
        self.session.commit()
        return uid


    def set_stats(self, version_uid, version_name, version_size_bytes,
            version_size_blocks, bytes_read, blocks_read, bytes_written,
            blocks_written, bytes_found_dedup, blocks_found_dedup,
            bytes_sparse, blocks_sparse, duration_seconds):
        stats = Stats(
            version_uid=version_uid,
            version_name=version_name,
            version_size_bytes=version_size_bytes,
            version_size_blocks=version_size_blocks,
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
        version.valid = 0
        self.session.commit()
        logger.info('Marked version invalid (UID {})'.format(
            uid,
            ))


    def set_version_valid(self, uid):
        version = self.get_version(uid)
        version.valid = 1
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
        version.protected = 1
        self.session.commit()
        logger.debug('Marked version protected (UID {})'.format(
            uid,
            ))


    def unprotect_version(self, uid):
        version = self.get_version(uid)
        version.protected = 0
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
        valid = 1 if valid else 0
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


    def set_blocks_invalid(self, uid, checksum):
        _affected_version_uids = self.session.query(distinct(Block.version_uid)).filter_by(uid=uid, checksum=checksum).all()
        affected_version_uids = [v[0] for v in _affected_version_uids]
        self.session.query(Block).filter_by(uid=uid, checksum=checksum).update({'valid': 0}, synchronize_session='fetch')
        self.session.commit()
        logger.info('Marked block invalid (UID {}, Checksum {}. Affected versions: {}'.format(
            uid,
            checksum,
            ', '.join(affected_version_uids)
            ))
        for version_uid in affected_version_uids:
            self.set_version_invalid(version_uid)
        return affected_version_uids


    def get_block(self, uid):
        return self.session.query(Block).filter_by(uid=uid).first()


    def get_block_by_checksum(self, checksum):
        return self.session.query(Block).filter_by(checksum=checksum, valid=1).first()


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
                    delete_candidate=DELETE_CANDIDATE_MAYBE,
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


    def export(self, version_uid, f):
        blocks = self.get_blocks_by_version(version_uid)
        _csv = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        _csv.writerow(['backy2 Version {} metadata dump'.format(METADATA_VERSION)])
        version = self.get_version(version_uid)
        _csv.writerow([
            version.uid,
            version.date.strftime('%Y-%m-%d %H:%M:%S'),
            version.name,
            version.snapshot_name,
            version.size,
            version.size_bytes,
            version.valid,
            version.protected,
            ])
        for block in blocks:
            _csv.writerow([
                block.uid,
                block.version_uid,
                block.id,
                block.date.strftime('%Y-%m-%d %H:%M:%S'),
                block.checksum,
                block.size,
                block.valid,
                ])
        return _csv


    def import_(self, f):
        _csv = csv.reader(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        signature = next(_csv)
        if signature[0] == 'backy2 Version 2.1 metadata dump':
            self.import_2_1(_csv)
        elif signature[0] == 'backy2 Version 2.2 metadata dump':
            self.import_2_2(_csv)
        else:
            raise ValueError('Wrong import format.')


    def import_2_1(self, _csv):
        version_uid, version_date, version_name, version_size, version_size_bytes, version_valid = next(_csv)
        try:
            self.get_version(version_uid)
        except KeyError:
            pass  # does not exist
        else:
            raise KeyError('Version {} already exists and cannot be imported.'.format(version_uid))
        version = Version(
            uid=version_uid,
            date=datetime.datetime.strptime(version_date, '%Y-%m-%d %H:%M:%S'),
            name=version_name,
            snapshot_name='',
            size=version_size,
            size_bytes=version_size_bytes,
            valid=version_valid,
            protected=0,
            )
        self.session.add(version)
        for uid, version_uid, id, date, checksum, size, valid in _csv:
            block = Block(
                uid=uid,
                version_uid=version_uid,
                id=id,
                date=datetime.datetime.strptime(date, '%Y-%m-%d %H:%M:%S'),
                checksum=checksum,
                size=size,
                valid=valid,
            )
            self.session.add(block)
        self.session.commit()


    def import_2_2(self, _csv):
        version_uid, version_date, version_name, version_snapshot_name, version_size, version_size_bytes, version_valid, version_protected = next(_csv)
        try:
            self.get_version(version_uid)
        except KeyError:
            pass  # does not exist
        else:
            raise KeyError('Version {} already exists and cannot be imported.'.format(version_uid))
        version = Version(
            uid=version_uid,
            date=datetime.datetime.strptime(version_date, '%Y-%m-%d %H:%M:%S'),
            name=version_name,
            snapshot_name=version_snapshot_name,
            size=version_size,
            size_bytes=version_size_bytes,
            valid=version_valid,
            protected=version_protected,
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
            for uid, version_uid, id, date, checksum, size, valid in _csv:
                block = Block(
                    uid=uid,
                    version_uid=version_uid,
                    id=id,
                    date=datetime.datetime.strptime(date, '%Y-%m-%d %H:%M:%S'),
                    checksum=checksum,
                    size=size,
                    valid=valid,
                )
                self.session.add(block)
        except:  # see above
            self.rm_version(version_uid)
        finally:
            self.session.commit()


    def close(self):
        self.session.commit()
        self.session.close()


