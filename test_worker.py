import pytest
import worker
import models
from sqlalchemy import create_engine
from datetime import datetime
import mock
import unittest
import os
import factory
import factory.alchemy


@pytest.fixture()
def setup_path(monkeypatch):
    monkeypatch.setattr(worker,
                        'PATH_DOWNLOAD',
                        str(TestProcess.test_path_download))


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


@pytest.mark.usefixtures("connection", "db_session")
class LogFactory(factory.alchemy.SQLAlchemyModelFactory):
    class Meta():
        model = models.WorkerLog

        sqlalchemy_session = db_session

    id = factory.Sequence(lambda n: n)
    instanceid = u'i-6b62f69d'
    date_time = datetime.utcnow()
    statement = u'Test'
    value = u'True'


@pytest.fixture(scope='class')
def fake_job1(db_session):
    model_instance = models.UserJob_Model(
        jobstatus=0,
        starttime=datetime.utcnow(),
        lastmodified=datetime.utcnow()
    )
    db_session.add(model_instance)
    db_session.flush()


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


# # --module function tests
# def test_cleanup_downloads():
#     test_dir = os.getcwd() + '/testdir'

#     if not os.path.exists(test_dir):
#         os.makedirs(test_dir)

#     f = open(test_dir + '/test.txt', 'a')
#     f.write('this is a test')
#     f.close()
#     # cleanup_downloads returns True if works
#     assert worker.cleanup_downloads(test_dir)


# # --jobs queue
# @pytest.mark.usefixtures("connection", "db_session")
# class TestQueue(unittest.TestCase):

#     @pytest.fixture(autouse=True)
#     def setup_tmpdir(self, tmpdir):
#         self.tmpdir = tmpdir

#     class Fake_Job_Class():
#         def __init__(self, message_content, message_attributes):
#             self.message_content = message_content
#             self.message_attributes = message_attributes

#     message = {'job_id': {'string_value': 1, 'data_type': 'Number'},
#                'band_2': {'string_value': 3, 'data_type': 'Number'},
#                'band_3': {'string_value': 2, 'data_type': 'Number'},
#                'band_1': {'string_value': 4, 'data_type': 'Number'},
#                'scene_id': {'string_value': 'LC80470272015005LGN00',
#                             'data_type': 'String'},
#                'email': {'string_value': 'test@test.com',
#                          'data_type': 'String'}}

#     fake_job_for_queue = [Fake_Job_Class("job", message)]
#     bad_fake_job = [Fake_Job_Class("job", ['test'])]

#     def test_get_job_attributes_returns_correctly(self):
#         result = worker.get_job_attributes(self.fake_job_for_queue)
#         assert result == (
#             {'job_id': 1, 'band_2': 3, 'band_3': 2, 'band_1': 4,
#              'scene_id': 'LC80470272015005LGN00', 'email': 'test@test.com'})


# # --process tests
# @pytest.mark.usefixtures("setup_dirs")
# class TestImageFiles(unittest.TestCase):
#     """These tests require real files"""

#     @mock.patch('worker.worker.Downloader')
#     def test_download_returns_correct_values(self, Downloader):
#         bands, input_path, scene_id = (worker.download_and_set(
#             TestProcess.fake_job_message))
#         self.assertEqual(input_path,
#                          TestProcess.test_path_download + '/' +
#                          TestProcess.test_scene_id)
#         self.assertEqual(bands, TestProcess.test_bands)
#         self.assertEqual(scene_id, TestProcess.test_scene_id)

@pytest.mark.usefixtures("setup_path")
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

    test_bands = [u'4', u'3', u'2']
    bad_test_bands = [u'4', u'3']

    test_scene_id = 'LC80470272015005LGN00'
    test_path_download = os.getcwd() + '/test_download'
    test_input_path = os.path.join(test_path_download, test_scene_id)

    test_band2_tif = '{}_B2.TIF'.format(test_scene_id)
    test_band3_tif = '{}_B3.TIF'.format(test_scene_id)
    test_band4_tif = '{}_B4.TIF'.format(test_scene_id)

    test_band2_tif_filename2 = '{}.re'.format(test_band2_tif)
    test_band3_tif_filename2 = '{}.re'.format(test_band3_tif)
    test_band4_tif_filename2 = '{}.re'.format(test_band4_tif)

    test_file_name = '{}_bands_432'.format(test_scene_id)

    test_file_tif = '{}.TIF'.format(test_file_name)
    test_file_png = '{}.png'.format(test_file_name)
    test_file_zip = '{}.zip'.format(test_file_name)

    test_path_to_tif = os.path.join(test_input_path, test_file_tif)
    test_path_to_png = os.path.join(test_input_path, test_file_png)
    test_path_to_zip = os.path.join(test_input_path, test_file_zip)

    test_file_pre_png = 'pre_{}'.format(test_file_png)
    test_file_pre_tif = 'pre_{}'.format(test_file_tif)

    test_preview_bucket = 'snapsatpreviews'
    test_full_bucket = 'snapsatcomposites'
    test_rendertype_preview = 'preview'
    test_rendertype_full = 'full'

    @classmethod
    def setup_class(cls):
        from zipfile import ZipFile

        if not os.path.exists(TestProcess.test_input_path):
            os.makedirs(TestProcess.test_input_path)
            try:
                with ZipFile('test_tiffs_Archive.zip', 'r') as zip_file:
                    zip_file.extractall(TestProcess.test_input_path)
            except IOError:
                print("Archive does not exist - downloading files")
                bands, input_path, scene_id = worker.download_and_set(
                    TestProcess.fake_job_message)

    @classmethod
    def teardown_class(cls):
        from shutil import rmtree

        rmtree(TestProcess.test_path_download)

    @mock.patch('worker.worker.Downloader')
    def test_download_and_set(self, Downloader):
        bands, input_path, scene_id = (worker.download_and_set(
                                       self.fake_job_message))

        onlyfiles = [f for f in os.listdir(self.test_input_path)
                     if os.path.isfile(os.path.join(self.test_input_path, f))]
        self.assertIn(self.test_band2_tif, onlyfiles)
        self.assertIn(self.test_band3_tif, onlyfiles)
        self.assertIn(self.test_band4_tif, onlyfiles)

    @mock.patch('worker.worker.Downloader')
    def test_download_and_set_errors_correctly(self, Downloader):
        with pytest.raises(Exception):
            bands, input_path, scene_id = (worker.download_and_set(
                self.bad_job_message))

    # --preview process tests
    def test_preview_resize_bands(self):
        """If test files don't exist, make them exist.

        The files are either downloaded from a fileserver, or unzipped
        from an archive file if it exists.
        """
        delete_me, rename_me = (
            worker.resize_bands(self.fake_job_message,
                                self.test_bands,
                                self.test_input_path,
                                self.test_scene_id))
        expected_delete_me = (
            [os.path.join(self.test_input_path,
                          '{}_B4.TIF'.format(self.test_scene_id)),
             os.path.join(self.test_input_path,
                          '{}_B3.TIF'.format(self.test_scene_id)),
             os.path.join(self.test_input_path,
                          '{}_B2.TIF'.format(self.test_scene_id))])
        self.assertEqual(delete_me, expected_delete_me)

        onlyfiles = [f for f in os.listdir(self.test_input_path)
                     if os.path.isfile(os.path.join(self.test_input_path, f))]
        self.assertIn(self.test_band2_tif_filename2, onlyfiles)
        self.assertIn(self.test_band3_tif_filename2, onlyfiles)
        self.assertIn(self.test_band4_tif_filename2, onlyfiles)

#     def test_preview_resize_bands_fails_with_message(self):
#         with pytest.raises(Exception) as e:
#             delete_me, rename_me = (
#                 worker.resize_bands(self.fake_job_message,
#                                     self.bad_test_bands,
#                                     '',
#                                     self.test_scene_id)
#             )
#         print(e.value)
#         assert 'gdal_translate did not downsize images' in str(e.value)

#     @mock.patch('worker.worker.os')
#     def test_preview_remove_and_rename(self, mock_os):
#         worker.remove_and_rename(['filelist1'], ['filelist2'])
#         mock_os.remove.assert_called_with('filelist1')
#         mock_os.rename.assert_called_with('filelist2', 'filelist1')

    def test_merge_images(self):
        worker.merge_images(self.fake_job_message,
                            self.test_input_path,
                            self.test_bands)
        onlyfiles = [f for f in os.listdir(self.test_input_path)
                     if os.path.isfile(os.path.join(self.test_input_path, f))]
        self.assertIn(self.test_file_tif, onlyfiles)

    def test_merge_images_fails_with_exception(self):
        worker.Process.side_effect = Exception()
        with pytest.raises(Exception) as e:
            worker.merge_images(self.fake_job_message, '', self.bad_test_bands)
        assert 'Merge images failed' in str(e.value)

    def test_preview_name_files(self):
        file_pre_png, path_to_tif, path_to_png = (
            worker.name_files(self.test_bands,
                              self.test_input_path,
                              self.test_scene_id,
                              self.test_rendertype_preview))
        assert file_pre_png == self.test_file_pre_png
        assert path_to_tif == (
            self.test_input_path + '/' + self.test_file_tif)
        assert path_to_png == (
            self.test_input_path + '/' + self.test_file_png)

    def test_preview_name_files_error(self):
        with pytest.raises(Exception) as e:
            file_pre_png, path_to_tif, path_to_png = (
                worker.name_files(self.test_bands,
                                  None,
                                  self.test_scene_id,
                                  self.test_rendertype_preview))
        assert 'File name creation failed' in str(e.value)

    def test_preview_tif_to_png(self):
        worker.tif_to_png(self.test_path_to_tif, self.test_path_to_png)
        onlyfiles = [f for f in os.listdir(self.test_input_path)
                     if os.path.isfile(os.path.join(self.test_input_path, f))]
        self.assertIn(self.test_file_png, onlyfiles)

    def test_preview_tif_to_png_error(self):
        with pytest.raises(Exception) as e:
            worker.tif_to_png(None, self.test_path_to_png)
        assert 'Tif to png failed' in str(e.value)

    # --full process tests
    def test_full_name_files(self):
        file_tif, path_to_tif, path_to_zip = (
            worker.name_files(self.test_bands,
                              self.test_input_path,
                              self.test_scene_id,
                              self.test_rendertype_full))
        assert file_tif == self.test_file_tif
        assert path_to_tif == (
            self.test_input_path + '/' + self.test_file_tif)
        assert path_to_zip == (
            self.test_input_path + '/' + self.test_file_zip)

    def test_full_name_files_error(self):
        with pytest.raises(Exception) as e:
            file_pre_png, path_to_tif, path_to_png = (
                worker.name_files(self.test_bands,
                                  None,
                                  self.test_scene_id,
                                  self.test_rendertype_full))
        assert 'File name creation failed' in str(e.value)

#     @mock.patch('worker.worker.zipfile')
#     def test_full_zip_file(self, mock_zipfile):
#         worker.zip_file(self.fake_job_message,
#                         self.test_file_tif,
#                         self.test_path_to_tif,
#                         self.test_path_to_zip)
#         mock_zipfile.ZipFile.assert_called_with(self.test_path_to_zip,
#                                                 'w',
#                                                 mock_zipfile.ZIP_DEFLATED)

    # --process tests
    

#     @mock.patch('worker.worker.Key')
#     @mock.patch('worker.worker.boto')
#     def test_upload_to_s3(self, boto, Key):
#         self.assertIsNone(worker.upload_to_s3(self.test_file_pre_png,
#                                               self.test_path_to_png,
#                                               self.fake_job_message,
#                                               self.test_preview_bucket))

#     @mock.patch('worker.worker.Key')
#     @mock.patch('worker.worker.boto')
#     def test_upload_to_s3_fails_with_exception(self, boto, Key):
#         # missing job argument to cause exception
#         worker.boto.connect_s3.side_effect = Exception()
#         with pytest.raises(Exception) as e:
#             worker.upload_to_s3(None,
#                                 '',
#                                 self.bad_job_message,
#                                 self.test_preview_bucket)
#         assert 'S3 Upload failed' in str(e.value)

#     @mock.patch('worker.worker.rmtree')
#     def test_delete_files(self, mock_rmtree):
#         worker.delete_files(self.test_input_path)
#         mock_rmtree.assert_called_with(self.test_input_path)
#         # check error checking:
#         worker.rmtree.side_effect = Exception(OSError)
#         with pytest.raises(Exception):
#             worker.delete_files('files')


# @mock.patch('worker.worker.Key')
# @mock.patch('worker.worker.boto')
# def test_whole_process_run_preview(Key, boto, setup_dirs):

#     result = worker.process(TestProcess.fake_job_message,
#                             TestProcess.test_preview_bucket,
#                             TestProcess.test_rendertype_preview)
#     # worker.process returns True if it works:
#     assert result


# @mock.patch('worker.worker.Key')
# @mock.patch('worker.worker.boto')
# def test_whole_process_run_full(Key, boto, setup_dirs):

#     result = worker.process(TestProcess.fake_job_message,
#                             TestProcess.test_full_bucket,
#                             TestProcess.test_rendertype_full)
#     # worker.process returns True if it works:
#     assert result
