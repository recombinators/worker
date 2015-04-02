import sys
sys.path.append('../landsat-util/landsat')
import pytest
from landsat_worker import render_1, db_sql
from sqlalchemy import create_engine
from datetime import datetime
import mock


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

@pytest.fixture
def fake_job1()


class TestProcess():

    def test_download_returns_correct_values():
        
