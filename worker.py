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


def process_image(job_attributes):
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


def download_and_set(job):
    """Download 3 band files for the given sceneid"""
    # set worker instance id for job
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


def merge_images(job, input_path, bands):
    """Combine the 3 bands into 1 color image"""
    UserJob_Model.set_job_status(job['job_id'], 2)
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

    if rendertype == u'preview':
        file_name = '{}_bands_{}'.format(scene_id, band_output)
        file_tif = '{}.TIF'.format(os.path.join(input_path, file_name))
        file_location = '{}png'.format(file_tif[:-3])
        return file_location, file_name, file_tif
    elif rendertype == u'full':
        file_name = '{}_bands_{}.TIF'.format(scene_id, band_output)
        file_location = os.path.join(input_path, file_name)
        return band_output, file_location

############################
# full
############################


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


def process(job):
    """
    Given bands and sceneID, download, image process, zip & upload to S3.
    """

    # set worker instance id for job
    UserJob_Model.set_worker_instance_id(job['job_id'], INSTANCE_ID)
    # download and set vars
    input_path, bands, scene_id = download_and_set(job, PATH_DOWNLOAD)

    # call landsat-util to merge images
    band_output, file_location = merge_images(
        job, input_path, bands, PATH_DOWNLOAD, scene_id
    )

    # zip file, maintain location
    file_name_zip = zip_file(job, band_output, scene_id, input_path,
                             file_location)

    # upload to s3
    file_location = upload_to_s3(file_location, file_name_zip, input_path, job)

    # delete files
    try:
        rmtree(input_path)           # band images and composite
    except OSError:
        print input_path
        print 'error deleting files'

    return True


############################
# preview
############################

def resize_bands(job, bands, input_path, scene_id):
    """gdal resizes each band file and returns filenames to delete and rename"""
    UserJob_Model.set_job_status(job['job_id'], 3)
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
