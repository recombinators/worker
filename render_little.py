import os
import sys
sys.path.append('landsat-util/landsat')
import boto
import subprocess
from downloader import Downloader
from image import Process
from boto.s3.key import Key
from shutil import rmtree
from datetime import datetime
from models import UserJob_Model
from boto import utils
import socket
from sqs import (make_SQS_connection, get_queue, get_message, get_attributes,
                 delete_message_from_handle)


os.getcwd()
PATH_DOWNLOAD = os.getcwd() + '/download'
PATH_ERROR_LOG = os.getcwd() + '/logs' + '/error_log.txt'
PATH_ACTIVITY_LOG = os.getcwd() + '/logs' + '/activity_log.txt'

AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
JOBS_QUEUE = 'snapsat_preview_queue'
REGION = 'us-west-2'

try:
    INSTANCE_METADATA = utils.get_instance_metadata(timeout=0.5, num_retries=1)
    INSTANCE_ID = INSTANCE_METADATA['instance-id']
except:
    INSTANCE_ID = socket.gethostname()


def cleanup_downloads(folder_path):
    """Clean up download folder if process fails.

    Return True if download folder empty.
    """
    for file_object in os.listdir(folder_path):
        file_object_path = os.path.join(folder_path, file_object)
        if os.path.isfile(file_object_path):
            os.remove(file_object_path)
        else:
            rmtree(file_object_path)
    if not os.listdir(folder_path):
        return True
    else:
        return False


def write_activity(message):
    """Write to activity log."""
    fo = open(PATH_ACTIVITY_LOG, 'a')
    fo.write('[{}] {}\n'.format(datetime.utcnow(), message))
    fo.close()


def write_error(message):
    """Write to error log."""
    fo = open(PATH_ERROR_LOG, 'a')
    fo.write('[{}] {}\n'.format(datetime.utcnow(), message))
    fo.close()


def checking_for_jobs():
    """Poll jobs queue for jobs."""
    SQSconn = make_SQS_connection(REGION, AWS_ACCESS_KEY_ID,
                                  AWS_SECRET_ACCESS_KEY)
    write_activity(SQSconn)
    jobs_queue = get_queue(SQSconn, JOBS_QUEUE)
    write_activity(jobs_queue)
    while True:
        job_message = get_message(jobs_queue)
        if job_message:
            try:
                job_attributes = get_attributes(job_message[0])
                write_activity(job_attributes)
            except Exception as e:
                write_activity('Attribute retrieval fail because {}'
                               .format(e.message))
                write_error('Attribute retrieval fail because {}'
                            .format(e.message))
                write_activity('Attribute retrieval traceback: {}'
                               .format(sys.exc_info()))
                write_error('Attribute retrieval traceback: {}'
                            .format(sys.exc_info()))

            try:
                del_status = delete_message_from_handle(SQSconn,
                                                        jobs_queue,
                                                        job_message[0])
                write_activity('Delete success = {}'.format(del_status))
            except Exception as e:
                write_activity('Delete success = {}'.format(del_status))
                write_activity('Delete message fail because {}'
                               .format(e.message))
                write_error('Delete message fail because {}'.format(e.message))
                write_activity('Delete traceback: {}'.format(sys.exc_info()))
                write_error('Delete traceback: {}'.format(sys.exc_info()))

            # Process full res images
            try:
                proc_status = process(job_attributes)
                write_activity('Job Process success = {}'.format(proc_status))
            except Exception as e:
                # If processing fails, send message to pyramid to update db
                write_activity('Job process success = {}'.format(False))
                write_activity('Job process fail because {}'
                               .format(e.message))
                write_error('Job process fail because {}'.format(e.message))
                write_activity('Job proceess traceback: {}'
                               .format(sys.exc_info()))
                write_error('Job process traceback: {}'.format(sys.exc_info()))

                cleanup_status = cleanup_downloads(PATH_DOWNLOAD)
                write_activity('Cleanup downloads success = {}'
                               .format(cleanup_status))
                write_error('Cleanup downloads success = {}'
                            .format(cleanup_status))


# begin process() breakdown here:
def download_and_set(job):
    """Download 3 band files for the given sceneid"""
    # set worker instance id for job
    UserJob_Model.set_worker_instance_id(job['job_id'], INSTANCE_ID)

    scene_id = str(job['scene_id'])
    input_path = os.path.join(PATH_DOWNLOAD, scene_id)
    # Create a subdirectory
    if not os.path.exists(input_path):
        os.makedirs(input_path)
        print 'Directory created.'

    try:
        b = Downloader(verbose=False, download_dir=PATH_DOWNLOAD)
        bands = [job['band_1'], job['band_2'], job['band_3']]
        b.download([scene_id], bands)
        print 'Finished downloading.'
    except:
        raise Exception('Download failed')
    return bands, input_path, scene_id


def resize_bands(bands, input_path, scene_id):
    """gdal resizes each band file and returns filenames to delete and rename"""
    delete_me, rename_me = [], []
    # Resize each band
    for band in bands:
        file_name = '{}/{}_B{}.TIF'.format(input_path, scene_id, band)
        delete_me.append(file_name)
        file_name2 = '{}.re'.format(file_name)
        rename_me.append(file_name2)
        subprocess.call(['gdal_translate', '-outsize', '10%', '10%',
                         file_name, file_name2])
        if not os.path.exists(file_name2):
            raise Exception('gdal_translate did not downsize images')
    print 'Finished resizing three images.'
    return delete_me, rename_me


def remove_and_rename(delete_me, rename_me):
    """delete and rename files"""
    for i, o in zip(rename_me, delete_me):
        os.remove(o)
        os.rename(i, o)


def merge_images(input_path, bands):
    """Combine the 3 bands into 1 color image"""
    try:
        processor = Process(input_path, bands=bands, dst_path=PATH_DOWNLOAD,
                            verbose=False)
        processor.run(pansharpen=False)
    except:
        raise Exception('Processing/landsat-util failed')


def name_files(bands, input_path, scene_id):
    """Give filenames to files for each band """
    band_output = ''
    for i in bands:
        band_output = '{}{}'.format(band_output, i)
    file_name = '{}_bands_{}'.format(scene_id, band_output)
    file_tif = '{}.TIF'.format(os.path.join(input_path, file_name))
    file_location = '{}png'.format(file_tif[:-3])
    return file_location, file_name, file_tif


def tif_to_png(file_location, file_name, file_tif):
    """Convert a tif file to a png"""
    subprocess.call(['convert', file_tif, file_location])
    file_png = 'pre_{}.png'.format(file_name)
    return file_png


def upload_to_s3(file_location, file_png, job):
    """Upload the processed file to S3, update job database"""
    try:
        print 'Uploading to S3'
        conne = boto.connect_s3(aws_access_key_id=AWS_ACCESS_KEY_ID,
                                aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
        b = conne.get_bucket('snapsatpreviews')
        k = Key(b)
        k.key = file_png
        k.set_contents_from_filename(file_location)
        k.get_contents_to_filename(file_location)
        hello = b.get_key(file_png)
        # make public
        hello.set_canned_acl('public-read')
        out = unicode(hello.generate_url(0, query_auth=False, force_http=True))
        print out
        UserJob_Model.set_job_status(job['job_id'], 5, out)
    except:
        raise Exception('S3 Upload failed')


def delete_files(input_path):
    """Remove leftover files when we are done with them."""
    try:
        rmtree(input_path)
    except OSError:
        print input_path
        print 'error deleting files'


def process(job):
    """Given bands and sceneID, download, image process, zip & upload to S3."""
    # download and set vars
    bands, input_path, scene_id = download_and_set(job)

    # resize bands
    delete_me, rename_me = resize_bands(bands, input_path, scene_id)

    # remove original band files and rename downsized to correct name
    remove_and_rename(delete_me, rename_me)

    # call landsat-util to merge images
    merge_images(input_path, bands)

    # construct the file names
    file_location, file_name, file_tif = name_files(bands,
                                                    input_path,
                                                    scene_id)

    # convert from TIF to png
    file_png = tif_to_png(file_location, file_name, file_tif)

    # upload to s3
    upload_to_s3(file_location, file_png, job)

    # delete files
    delete_files(input_path)

    return True

if __name__ == '__main__':
    checking_for_jobs()
