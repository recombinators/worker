import os
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
COMPOSITE_QUEUE = 'snapsat_composite_queue'
REGION = 'us-west-2'

try:
    INSTANCE_METADATA = utils.get_instance_metadata(timeout=0.5, num_retries=1)
    INSTANCE_ID = INSTANCE_METADATA['instance-id']
except:
    INSTANCE_ID = socket.gethostname()


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


# Begin checking for jobs
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


def upload_to_s3(file_location, file_name_zip, input_path, job):
    """
    Upload processed images to S3.
    """
    try:
        print 'Uploading to S3'
        UserJob_Model.set_job_status(job['job_id'], 4)
        file_location = os.path.join(input_path, file_name_zip)
        conne = boto.connect_s3(aws_access_key_id=AWS_ACCESS_KEY_ID,
                                aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
        b = conne.get_bucket('snapsatcomposites')
        k = Key(b)
        k.key = file_name_zip
        k.set_contents_from_filename(file_location)
        k.get_contents_to_filename(file_location)
        hello = b.get_key(file_name_zip)
        # make public
        hello.set_canned_acl('public-read')
        out = unicode(hello.generate_url(0, query_auth=False, force_http=True))
        print out
        UserJob_Model.set_job_status(job['job_id'], 5, out)
    except:
        raise Exception('S3 Upload failed')
    return file_location


def merge_images(job, input_path, bands, PATH_DOWNLOAD, scene_id):
    """Process images using landsat-util."""
    UserJob_Model.set_job_status(job['job_id'], 2)
    c = Process(input_path, bands=bands, dst_path=PATH_DOWNLOAD, verbose=False)
    c.run(pansharpen=False)
    band_output = ''
    for band in bands:
        band_output = '{}{}'.format(band_output, band)
    file_name = '{}_bands_{}.TIF'.format(scene_id, band_output)
    file_location = os.path.join(input_path, file_name)
    return band_output, file_location


def zip_file(job, band_output, scene_id, input_path, file_location):
    """
    Compress the image.
    """
    print 'Zipping file'
    UserJob_Model.set_job_status(job['job_id'], 3)
    file_name_zip = '{}_bands_{}.zip'.format(scene_id, band_output)
    file_name = '{}_bands_{}.TIF'.format(scene_id, band_output)
    path_to_zip = os.path.join(input_path, file_name_zip)
    with zipfile.ZipFile(path_to_zip, 'w', zipfile.ZIP_DEFLATED) as myzip:
        myzip.write(file_location, arcname=file_name)
    return file_name_zip


def download_and_set(job, PATH_DOWNLOAD):
    """Download the image file."""
    UserJob_Model.set_job_status(job['job_id'], 1)
    b = Downloader(verbose=False, download_dir=PATH_DOWNLOAD)
    scene_id = str(job['scene_id'])
    bands = [job['band_1'], job['band_2'], job['band_3']]
    b.download([scene_id], bands)
    input_path = os.path.join(PATH_DOWNLOAD, scene_id)
    return input_path, bands, scene_id


def process_image(job_attributes, process):
    """Begin the image processing and log the results."""
    try:
        proc_status = process(job_attributes)
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


def checking_for_jobs(JOBS_QUEUE):
    """Poll jobs queue for jobs."""
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
            process_image(job_attributes)


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