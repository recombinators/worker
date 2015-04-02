import sys
sys.path.append('../landsat-util/landsat')
import pytest
from landsat_worker import render_1, db_sql
from sqlalchemy import create_engine
from datetime import datetime


@pytest.fixture(scope='session')
def connection(request):
    engine = create_engine('postgresql://postgres@/test_bar')
    db_sql.Base.metadata.create_all(engine)
    connection = engine.connect()
    db_sql.DBSession.registry.clear()
    db_sql.DBSession.configure(bind=connection)
    db_sql.Base.metadata.bind = engine
    request.addfinalizer(db_sql.Base.metadata.drop_all)
    return connection


@pytest.fixture
def db_session(request, connection):
    from transaction import abort
    trans = connection.begin()
    request.addfinalizer(trans.rollback)
    request.addfinalizer(abort)

    from landsat_worker.db_sql import DBSession
    return DBSession


def test_db_lookup(db_session):
    model_instance = db_sql.UserJob_Model(jobstatus=0, starttime=datetime.utcnow(), lastmodified=datetime.utcnow())
    db_session.add(model_instance)
    db_session.flush()

    assert 1 == db_session.query(db_sql.UserJob_Model).count()


def test_db_is_rolled_back(db_session):
    assert 0 == db_session.query(db_sql.UserJob_Model).count()
