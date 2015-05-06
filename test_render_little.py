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


@pytest.fixture()
def setup_dirs(monkeypatch):
    from zipfile import ZipFile
    from shutil import rmtree
    monkeypatch.setattr(render_little,
                        'PATH_DOWNLOAD',
                        str(TestProcess.test_tmp_download)
                        )

    if os.path.exists(TestProcess.test_tmp_download):
        rmtree(TestProcess.test_tmp_download)
    if not os.path.exists(TestProcess.test_input_path):
        os.makedirs(TestProcess.test_input_path)
        try:
            with ZipFile('test_tiffs_Archive.zip', 'r') as zip_file:
                zip_file.extractall(TestProcess.test_input_path)
        except IOError:
            print("Archive does not exist - downloading files")
            bands, input_path, scene_id = render_little.download_and_set(
                TestProcess.fake_job_message)


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


@pytest.fixture()
def write_activity_fix(monkeypatch, tmpdir):
    tmp_activity_log = tmpdir.mkdir('log').join('tmp_act_log.txt')
    monkeypatch.setattr(render_little,
                        'PATH_ACTIVITY_LOG',
                        str(tmp_activity_log)
                        )
    return tmp_activity_log


@pytest.fixture()
def write_error_fix(monkeypatch, tmpdir):
    if tmpdir.exists():
        tmp_error_log = tmpdir.join('tmp_error_log.txt')
    else:
        tmp_error_log = tmpdir.mkdir('log').join('tmp_error_log.txt')
    monkeypatch.setattr(render_little,
                        'PATH_ERROR_LOG',
                        str(tmp_error_log)
                        )
    return tmp_error_log


# --test db functionality tests
def test_db_lookup(db_session):
    model_instance = models.UserJob_Model(jobstatus=0,
                                          starttime=datetime.utcnow(),
                                          lastmodified=datetime.utcnow())
    db_session.add(model_instance)
    db_session.flush()

    assert 1 == db_session.query(models.UserJob_Model).count()


def test_db_is_rolled_back(db_session):
    assert 0 == db_session.query(models.UserJob_Model).count()


# --module function tests
def test_cleanup_downloads():
    test_dir = os.getcwd() + '/testdir'

    if not os.path.exists(test_dir):
        os.makedirs(test_dir)

    f = open(test_dir + '/test.txt', 'a')
    f.write('this is a test')
    f.close()
    # cleanup_downloads returns True if works
    assert render_little.cleanup_downloads(test_dir)


def test_write_activity(write_activity_fix):
    render_little.write_activity('test message')
    assert 'test message' in write_activity_fix.read()


def test_write_error(write_error_fix):
    render_little.write_error('test message')
    assert 'test message' in write_error_fix.read()


# --jobs queue
@pytest.mark.usefixtures("connection", "db_session", "fake_job1",
                         "write_activity_fix", "write_error_fix")
class TestQueue(unittest.TestCase):

    class Fake_Job_Class():
        def __init__(self, message_content, message_attributes):
            self.message_content = message_content
            self.message_attributes = message_attributes

    message = {'job_id': {'string_value': 1, 'data_type': 'Number'},
               'band_2': {'string_value': 3, 'data_type': 'Number'},
               'band_3': {'string_value': 2, 'data_type': 'Number'},
               'band_1': {'string_value': 4, 'data_type': 'Number'},
               'scene_id': {'string_value': 'LC80470272015005LGN00',
               'data_type': 'String'},
               'email': {'string_value': 'test@test.com',
                         'data_type': 'String'}}

    fake_job_for_queue = [Fake_Job_Class("job", message)]
    bad_fake_job_for_queue = ['bad']

    @pytest.mark.usefixtures("write_activity_fix", "write_error_fix")
    def test_get_job_attributes_returns_correctly(self):
        result = render_little.get_job_attributes(self.fake_job_for_queue)
        assert result == (
            {'job_id': 1, 'band_2': 3, 'band_3': 2, 'band_1': 4, 'scene_id': 'LC80470272015005LGN00', 'email': 'test@test.com'})

    # The following test is being a mega-pain...

    #@mock.patch('worker.render_little.get_job_attributes')
    #def test_get_job_attributes_errors_correctly(self,
    #                                             get_job_attributes):
    #    render_little.get_job_attributes.side_effect = Exception()
    #    with pytest.raises(Exception):
    #        render_little.get_job_attributes(self.bad_fake_job_for_queue)
    #    assert "Attribute retrieval fail because" in write_activity_fix


# --process tests
@pytest.mark.usefixtures("setup_dirs")
class TestImageFiles(unittest.TestCase):
    """These tests require real files"""

    @mock.patch('worker.render_little.Downloader')
    def test_download_returns_correct_values(self, Downloader):
        bands, input_path, scene_id = (render_little.download_and_set(
            TestProcess.fake_job_message))
        self.assertEqual(input_path,
                         os.getcwd() + '/test_download/LC80470272015005LGN00')
        self.assertEqual(bands, [u'4', u'3', u'2'])
        self.assertEqual(scene_id, 'LC80470272015005LGN00')


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

    test_tmp_download = os.getcwd() + '/test_download'
    test_input_path = os.getcwd() + '/test_download/LC80470272015005LGN00'
    test_bands = [u'4', u'3', u'2']
    bad_test_bands = [u'4', u'3']
    test_scene_id = 'LC80470272015005LGN00'
    test_file_location = (os.getcwd() +
        '/download/LC80470272015005LGN00/LC80470272015005LGN00_bands_432.TIF')
    test_file_name = 'LC80470272015005LGN00_bands_432'
    test_file_name_zip = 'LC80470272015005LGN00_bands_432.zip'
    test_file_png = 'pre_LC80470272015005LGN00_bands_432.png'
    test_file_tif = 'pre_LC80470272015005LGN00_bands_432.TIF'

    @mock.patch('worker.render_little.Downloader')
    def test_download_errors_correctly(self, Downloader):
        with pytest.raises(Exception):
            bands, input_path, scene_id = (render_little.download_and_set(
                self.bad_job_message))

    def test_resize_bands_creates_files(self):
        """If test files don't exist, make them exist

        The files are either downloaded from a fileserver, or unzipped
        from an archive file if it exists.
        """
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

    @mock.patch('worker.render_little.os')
    def test_remove_and_rename(self, mock_os):
        render_little.remove_and_rename(['filelist1'], ['filelist2'])
        mock_os.remove.assert_called_with('filelist1')
        mock_os.rename.assert_called_with('filelist2', 'filelist1')

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

    def test_name_files(self):
        file_location, file_name, file_tif = (
            render_little.name_files(self.test_bands,
                                     self.test_input_path,
                                     self.test_scene_id))
        assert file_location == (
            self.test_input_path + '/LC80470272015005LGN00_bands_432.png')
        assert file_name == 'LC80470272015005LGN00_bands_432'
        assert file_tif == (
            self.test_input_path + '/LC80470272015005LGN00_bands_432.TIF')

    @mock.patch('worker.render_little.subprocess')
    def test_tif_to_png(self, mock_subp):
        file_png = render_little.tif_to_png(self.test_file_location,
                                            self.test_file_name,
                                            self.test_file_tif)
        assert file_png == self.test_file_png
        mock_subp.call.assert_called_with(['convert',
                                           self.test_file_tif,
                                           self.test_file_location])

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
        render_little.boto.connect_s3.side_effect = Exception()
        with pytest.raises(Exception) as e:
            render_little.upload_to_s3(None,
                                       '',
                                       self.bad_job_message,
                                       )
        assert 'S3 Upload failed' in str(e.value)

    @mock.patch('worker.render_little.rmtree')
    def test_delete_files(self, mock_rmtree):
        render_little.delete_files(self.test_input_path)
        mock_rmtree.assert_called_with(self.test_input_path)
        # check error checking:
        render_little.rmtree.side_effect = Exception(OSError)
        with pytest.raises(Exception):
            render_little.delete_files('files')


@mock.patch('worker.render_little.Key')
@mock.patch('worker.render_little.boto')
def test_whole_process_run(Key, boto, setup_dirs):

    result = render_little.process(TestProcess.fake_job_message)
    # render_little.process returns True if it works:
    assert result
