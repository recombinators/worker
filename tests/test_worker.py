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


@pytest.fixture(scope='module')
def fake_job1(request):
    model_instance = db_sql.UserJob_Model(jobstatus=0,
                                          starttime=datetime.utcnow(),
                                          lastmodified=datetime.utcnow())
    db_session.add(model_instance)
    db_session.flush()


class TestProcess(fake_job1):
    fake_job_message = {u'job_id': u'1',
                        u'band_2': u'3',
                        u'band_3': u'2',
                        u'band_1': u'4',
                        u'scene_id': u'LC80470272015005LGN00',
                        u'email': u'test@test.com'}

    def test_download_returns_correct_values():
        render_1.download_and_set(fake_job_message)
