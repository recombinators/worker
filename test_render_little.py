import sys
sys.path.append('./landsat-util/landsat')
import pytest
import render_little
import models
from sqlalchemy import create_engine
from datetime import datetime
import mock
import unittest
import os
import factory
import factory.alchemy


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

    # bad_job_message is missing band_1
    bad_job_message = {u'job_id': u'1',
                       u'band_2': u'3',
                       u'band_3': u'2',
                       u'scene_id': u'LC80470272015005LGN00',
                       u'email': u'test@test.com'}

    test_input_path = os.getcwd() + '/test_download/LC80470272015005LGN00'
    test_bands = [u'4', u'3', u'2']
    bad_test_bands = [u'4', u'3']
    test_scene_id = 'LC80470272015005LGN00'
    test_band_output = '432'
    test_file_location = (os.getcwd() +
        '/download/LC80470272015005LGN00/LC80470272015005LGN00_bands_432.TIF')
    test_file_name_zip = 'LC80470272015005LGN00_bands_432.zip'
    test_file_png = 'pre_LC80470272015005LGN00_bands_432.png'

    @mock.patch('worker.render_little.Downloader')
    def test_download_returns_correct_values(self, Downloader):
        bands, input_path, scene_id = (render_little.download_and_set(
            self.fake_job_message))
        self.assertEqual(input_path,
                         os.getcwd() + '/download/LC80470272015005LGN00')
        self.assertEqual(bands, [u'4', u'3', u'2'])
        self.assertEqual(scene_id, 'LC80470272015005LGN00')

    @mock.patch('worker.render_little.Downloader')
    def test_download_errors_correctly(self, Downloader):
        with pytest.raises(Exception):
            bands, input_path, scene_id = (render_little.download_and_set(
                self.bad_job_message))

    def test_resize_bands_creates_files(self):
        if not os.path.exists(self.test_input_path):
            os.makedirs(self.test_input_path)
        delete_me, rename_me = (
            render_little.resize_bands(self.test_bands, self.test_input_path,
                                       self.test_scene_id)
            )
        expected_delete_me = (
            [self.test_input_path + '/LC80470272015005LGN00_B4.TIF',
             self.test_input_path + '/LC80470272015005LGN00_B3.TIF',
             self.test_input_path + '/LC80470272015005LGN00_B2.TIF']
            )
        self.assertEqual(delete_me, expected_delete_me)

    def test_resize_bands_fails_with_message(self):
        with pytest.raises(Exception) as e:
            delete_me, rename_me = (
                render_little.resize_bands(self.bad_test_bands,
                                           '',
                                           self.test_scene_id)
            )
        print(e.value)
        assert 'gdal_translate did not downsize images' in str(e.value)

    @mock.patch('worker.render_little.Key')
    @mock.patch('worker.render_little.boto')
    def test_upload_to_s3(self, boto, Key):
        self.assertIsNone(render_little.upload_to_s3(self.test_file_location,
                                                     self.test_file_png,
                                                     self.fake_job_message
                                                     ))

    @mock.patch('worker.render_little.Key')
    @mock.patch('worker.render_little.boto')
    def test_upload_to_s3_fails_with_exception(self, boto, Key):
        # missing job argument to cause exception
        with self.assertRaises(Exception):
            render_little.upload_to_s3(self.test_file_location,
                                       self.test_file_name_zip,
                                       self.test_input_path,
                                       None
                                       )

    @mock.patch('worker.render_little.Process')
    def test_merge_images(self, Process):
        render_little.merge_images(self.test_input_path, self.test_bands)
        render_little.Process.assert_called_with(
            self.test_input_path,
            dst_path=render_little.PATH_DOWNLOAD,
            verbose=False,
            bands=self.test_bands
        )

    @mock.patch('worker.render_little.Process')
    def test_merge_images_fails_with_exception(self, Process):
        render_little.Process.side_effect = Exception()
        with pytest.raises(Exception) as e:
            render_little.merge_images('', self.bad_test_bands)
        assert 'Processing/landsat-util failed' in str(e.value)


def test_cleanup_downloads():
    test_dir = os.getcwd() + '/testdir'

    if not os.path.exists(test_dir):
        os.makedirs(test_dir)

    f = open(test_dir + '/test.txt', 'a')
    f.write('this is a test')
    f.close()

    assert render_little.cleanup_downloads(test_dir) == True
