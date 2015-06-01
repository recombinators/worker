import os
import sys
import boto
import subprocess
import zipfile
from landsat.downloader import Downloader
from landsat.landsat import Process
from boto.s3.key import Key
from shutil import rmtree
from datetime import datetime
from models import UserJob_Model, WorkerLog
from boto import utils
import socket
from sqs import (make_SQS_connection, get_queue, get_message, get_attributes,
                 delete_message_from_handle)

PATH_DOWNLOAD = os.getcwd() + '/download'

AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
PREVIEW_QUEUE = 'snapsat_preview_queue'
FULL_QUEUE = 'snapsat_composite_queue'
PREVIEW_BUCKET = 'snapsatpreviews'
FULL_BUCKET = 'snapsatcomposites'
REGION = 'us-west-2'

try:
    INSTANCE_METADATA = utils.get_instance_metadata(timeout=0.5, num_retries=1)
    INSTANCE_ID = INSTANCE_METADATA['instance-id']
except:
    INSTANCE_ID = socket.gethostname()

############################
# full and preview
############################


def checking_for_jobs(rendertype):
    """Poll jobs queue for jobs."""

    if rendertype == 'full':
        JOBS_QUEUE = FULL_QUEUE
        BUCKET = FULL_BUCKET
    elif rendertype == 'preview':
        JOBS_QUEUE = PREVIEW_QUEUE
        BUCKET = PREVIEW_BUCKET

    SQSconn = make_SQS_connection(REGION, AWS_ACCESS_KEY_ID,
                                  AWS_SECRET_ACCESS_KEY)
    write_activity('SQS Connection', SQSconn.server_name(), 'success')
    jobs_queue = get_queue(SQSconn, JOBS_QUEUE)
    write_activity('Jobs queue', jobs_queue.name, 'success')
    while True:
        job_message = get_message(jobs_queue)
        if job_message:
            job_attributes = get_job_attributes(job_message)
            delete_job_from_queue(SQSconn, job_message, jobs_queue)

            # Process full res images
            process_job(job_attributes, BUCKET, rendertype)


def process_job(job_attributes, BUCKET, rendertype):
    """Begin the image processing and log the results."""
    try:
        proc_status = process(job_attributes, BUCKET, rendertype)
        write_activity('Job process status',
                       unicode(proc_status), 'success')
    except Exception as e:
        proc_status = False
        # If processing fails, send message to pyramid to update db
        write_activity('Job process success',
                       unicode(proc_status), 'error')
        write_activity('Job process fail because',
                       e.message, 'error')
        cleanup_status = cleanup_downloads(PATH_DOWNLOAD)
        write_activity('Cleanup downloads success',
                       cleanup_status, 'error')
        UserJob_Model.set_job_status(job_attributes['job_id'], 10)


def process(job_attributes, BUCKET, rendertype):
    """Given bands and sceneID, download, image process, zip & upload to S3."""
    # set worker instance id for job
    UserJob_Model.set_worker_instance_id(job_attributes['job_id'], INSTANCE_ID)

    # download and set vars
    bands, input_path, scene_id = download_and_set(job_attributes)

    if rendertype == 'preview':
        # resize bands
        delete_me, rename_me = resize_bands(job_attributes,
                                            bands,
                                            input_path,
                                            scene_id)

        # remove original band files and rename downsized to correct name
        remove_and_rename(delete_me, rename_me)

        # call landsat-util to merge images
        merge_images(job_attributes, input_path, bands)

        # construct the file names
        file_pre_png, path_to_tif, path_to_png = name_files(bands,
                                                            input_path,
                                                            scene_id,
                                                            rendertype)

        # convert from TIF to png
        tif_to_png(path_to_tif, path_to_png)

        file_upload_name = file_pre_png
        file_to_upload = path_to_png

    elif rendertype == 'full':
        # call landsat-util to merge images
        merge_images(job_attributes, input_path, bands)

        # construct the file names
        file_tif, path_to_tif, path_to_zip = name_files(bands,
                                                        input_path,
                                                        scene_id,
                                                        rendertype)

        # zip file, maintain location
        file_zip = zip_file(job_attributes,
                            file_tif,
                            path_to_tif,
                            path_to_zip)

        file_upload_name = file_zip
        file_to_upload = path_to_zip

    # upload to s3
    upload_to_s3(file_to_upload, file_upload_name, job_attributes, BUCKET)

    # delete files
    delete_files(input_path)

    return True


def download_and_set(job_attributes):
    """Download 3 band files for the given sceneid"""
    # set worker instance id for job
    scene_id = str(job_attributes['scene_id'])
    input_path = os.path.join(PATH_DOWNLOAD, scene_id)
    # Create a subdirectory
    if not os.path.exists(input_path):
        os.makedirs(input_path)
        print 'Directory created.'

    try:
        UserJob_Model.set_job_status(job_attributes['job_id'], 1)
        b = Downloader(verbose=False, download_dir=PATH_DOWNLOAD)
        bands = [job_attributes['band_1'],
                 job_attributes['band_2'],
                 job_attributes['band_3']]
        b.download([scene_id], bands)
        print 'Finished downloading.'
    except:
        raise Exception('Download failed')
    return bands, input_path, scene_id


def merge_images(job_attributes, input_path, bands):
    """Combine the 3 bands into 1 color image"""
    UserJob_Model.set_job_status(job_attributes['job_id'], 2)
    try:
        processor = Process(input_path, bands=bands, dst_path=PATH_DOWNLOAD,
                            verbose=False)
        processor.run(pansharpen=False)
    except:
        raise Exception('Processing/landsat-util failed')


def name_files(bands, input_path, scene_id, rendertype):
    """Give filenames to files for each band """
    band_output = ''
    for i in bands:
        band_output = '{}{}'.format(band_output, i)

    file_name = '{}_bands_{}'.format(scene_id, band_output)

    file_tif = '{}.TIF'.format(file_name)
    path_to_tif = os.path.join(input_path, file_tif)

    if rendertype == 'preview':
        file_png = '{}.png'.format(file_name)
        path_to_png = os.path.join(input_path, file_png)
        file_pre_png = 'pre_{}.png'.format(file_name)
        return file_pre_png, path_to_tif, path_to_png
    elif rendertype == 'full':
        file_zip = '{}.zip'.format(file_name)
        path_to_zip = os.path.join(input_path, file_zip)
        return file_tif, path_to_tif, path_to_zip


def cleanup_downloads(folder_path):
    """Clean up download folder if process fails.

    Return True if the download folder is empty.
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


def write_activity(statement, value, activity_type):
    """Write to activity log."""
    WorkerLog.log_entry(INSTANCE_ID, statement, value, activity_type)


def get_job_attributes(job_message):
    """Get job attributes, log the result."""
    job_attributes = None
    try:
        job_attributes = get_attributes(job_message[0])
        write_activity('Job attributes',
                       str(job_attributes), 'success')
    except Exception as e:
        write_activity('Attribute retrieval fail because',
                       e.message, 'error')
    return job_attributes


def delete_job_from_queue(SQSconn, job_message, jobs_queue):
    """Remove the job from the job queue."""
    try:
        del_status = delete_message_from_handle(SQSconn,
                                                jobs_queue,
                                                job_message[0])
        write_activity('Delete status', unicode(del_status), 'success')
    except Exception as e:
        write_activity('Delete status', unicode(del_status), 'error')
        write_activity('Delete message fail because ',
                       e.message, 'error')


def upload_to_s3(file_to_upload, file_upload_name, job_attributes, BUCKET):
    """Upload the processed file to S3, update job database"""

    try:
        print 'Uploading to S3'
        UserJob_Model.set_job_status(job_attributes['job_id'], 4)
        conne = boto.connect_s3(aws_access_key_id=AWS_ACCESS_KEY_ID,
                                aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
        b = conne.get_bucket(BUCKET)
        k = Key(b)
        k.key = file_upload_name
        k.set_contents_from_filename(file_to_upload)
        k.get_contents_to_filename(file_to_upload)
        hello = b.get_key(file_upload_name)
        # make public
        hello.set_canned_acl('public-read')
        out = unicode(hello.generate_url(0, query_auth=False, force_http=True))
        print out
        UserJob_Model.set_job_status(job_attributes['job_id'], 5, out)
    except:
        raise Exception('S3 Upload failed')


def delete_files(input_path):
    """Remove leftover files when we are done with them."""
    try:
        rmtree(input_path)
    except OSError:
        print input_path
        print 'error deleting files'


############################
# full
############################


def zip_file(job_attributes, file_tif, path_to_tif, path_to_zip):
    """
    Compress the image.
    """
    print 'Zipping file'
    UserJob_Model.set_job_status(job_attributes['job_id'], 3)
    with zipfile.ZipFile(path_to_zip, 'w', zipfile.ZIP_DEFLATED) as myzip:
        myzip.write(path_to_tif, arcname=file_tif)


############################
# preview
############################

def resize_bands(job_attributes, bands, input_path, scene_id):
    """gdal resizes each band file and returns filenames to delete and rename"""
    UserJob_Model.set_job_status(job_attributes['job_id'], 3)
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


def tif_to_png(path_to_tif, path_to_png):
    """Convert a tif file to a png"""
    subprocess.call(['convert', path_to_tif, path_to_png])

if __name__ == '__main__':
    checking_for_jobs(sys.argv[1])
