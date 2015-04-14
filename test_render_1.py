import sys
import pytest
import render_1
import models
from sqlalchemy import create_engine
from datetime import datetime
import mock
import unittest
import os
import factory
import factory.alchemy
from models import RenderCache_Model, UserJob_Model

sys.path.append('../landsat-util/landsat')


@pytest.fixture(scope='session', autouse=True)
def connection(request):
    engine = create_engine('postgresql://postgres@/test_bar')
    models.Base.metadata.create_all(engine)
    connection = engine.connect()
    models.DBSession.registry.clear()
    models.DBSession.configure(bind=connection)
    models.Base.metadata.bind = engine
    request.addfinalizer(models.Base.metadata.drop_all)
    return connection


@pytest.fixture(autouse=True)
def db_session(request, connection):
    from transaction import abort
    trans = connection.begin()
    request.addfinalizer(trans.rollback)
    request.addfinalizer(abort)

    from models import DBSession
    return DBSession


@pytest.mark.usefixtures("connection", "db_session")
class JobFactory(factory.alchemy.SQLAlchemyModelFactory):
    class Meta():
        model = models.UserJob_Model

        sqlalchemy_session = db_session

    jobstatus = 0
    starttime = datetime.utcnow()
    lastmodified = datetime.utcnow()
    band1 = u'4'
    band2 = u'3'
    band3 = u'2'
    entityid = u'LC80470272015005LGN00'
    email = u'test@test.com'
    jobid = factory.Sequence(lambda n: n)


@pytest.fixture(scope='class')
def fake_job1(db_session):
    model_instance = models.UserJob_Model(
        jobstatus=0,
        starttime=datetime.utcnow(),
        lastmodified=datetime.utcnow()
    )
    db_session.add(model_instance)
    db_session.flush()

# --- test db functionality tests


def test_db_lookup(db_session):
    model_instance = models.UserJob_Model(jobstatus=0,
                                          starttime=datetime.utcnow(),
                                          lastmodified=datetime.utcnow())
    db_session.add(model_instance)
    db_session.flush()

    assert 1 == db_session.query(models.UserJob_Model).count()


def test_db_is_rolled_back(db_session):
    assert 0 == db_session.query(models.UserJob_Model).count()


# --- process tests

@pytest.mark.usefixtures("connection", "db_session", "fake_job1")
class TestProcess(unittest.TestCase):

    fake_job_message = {u'job_id': u'1',
                        u'band_2': u'3',
                        u'band_3': u'2',
                        u'band_1': u'4',
                        u'scene_id': u'LC80470272015005LGN00',
                        u'email': u'test@test.com'}

    test_input_path = os.getcwd() + '/download/LC80470272015005LGN00'
    test_bands = [u'4', u'3', u'2']
    test_scene_id = 'LC80470272015005LGN00'
    test_band_output = '432'
    test_file_location = (os.getcwd() +
        '/download/LC80470272015005LGN00/LC80470272015005LGN00_bands_432.TIF')
    test_file_name_zip = 'LC80470272015005LGN00_bands_432.zip'

    @mock.patch('recombinators_landsat.landsat_worker.render_1.Downloader')
    def test_download_returns_correct_values(self, Downloader):
        input_path, bands, scene_id = (render_1.download_and_set(
            self.fake_job_message, render_1.PATH_DOWNLOAD))
        self.assertEqual(input_path,
                         os.getcwd() + '/download/LC80470272015005LGN00')
        self.assertEqual(bands, [u'4', u'3', u'2'])
        self.assertEqual(scene_id, 'LC80470272015005LGN00')

    @mock.patch('recombinators_landsat.landsat_worker.render_1.Process')
    def test_process_image(self, Process):
        band_output, file_location = (render_1.process_image(
            self.fake_job_message,
            self.test_input_path,
            self.test_bands,
            render_1.PATH_DOWNLOAD,
            self.test_scene_id)
        )
        self.assertEqual(band_output, '432')
        self.assertEqual(
            file_location, os.getcwd() +
            '/download/LC80470272015005LGN00/LC80470272015005LGN00_bands_432.TIF')

    @mock.patch('recombinators_landsat.landsat_worker.render_1.zipfile')
    def test_zip_file(self, zipfile):
        file_name_zip = render_1.zip_file(self.fake_job_message,
                                          self.test_band_output,
                                          self.test_scene_id,
                                          self.test_input_path,
                                          self.test_file_location)
        self.assertEqual(file_name_zip, "LC80470272015005LGN00_bands_432.zip")

    @mock.patch('recombinators_landsat.landsat_worker.render_1.Key')
    @mock.patch('recombinators_landsat.landsat_worker.render_1.boto')
    def test_upload_to_s3(self, boto, Key):
        file_location = render_1.upload_to_s3(self.test_file_location,
                                              self.test_file_name_zip,
                                              self.test_input_path,
                                              self.fake_job_message
                                              )
        self.assertEqual(
            file_location, os.getcwd() +
            '/download/LC80470272015005LGN00/LC80470272015005LGN00_bands_432.zip')

    @mock.patch('recombinators_landsat.landsat_worker.render_1.Key')
    @mock.patch('recombinators_landsat.landsat_worker.render_1.boto')
    def test_upload_to_s3_fails_with_exception(self, boto, Key):
        # missing job argument to cause exception
        with self.assertRaises(Exception):
            render_1.upload_to_s3(self.test_file_location,
                                  self.test_file_name_zip,
                                  self.test_input_path,
                                  None
                                  )


def test_cleanup_downloads():
    test_dir = os.getcwd() + '/testdir'

    if not os.path.exists(test_dir):
        os.makedirs(test_dir)

    f = open(test_dir + '/test.txt', 'a')
    f.write('this is a test')
    f.close()

    assert render_1.cleanup_downloads(test_dir) == True
