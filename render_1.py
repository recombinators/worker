from landsat.downloader import Downloader
from landsat.landsat import Process
from boto.s3.key import Key
from shutil import rmtree
from datetime import datetime
from boto import utils
import socket
from models import UserJob_Model, WorkerLog
from sqs import (make_SQS_connection, get_queue, get_message,
                 get_attributes, delete_message_from_handle)
import os
import boto
import zipfile

from worker import FULL_QUEUE as JOBS_QUEUE
from worker import FULL_BUCKET as BUCKET
from worker import (PATH_DOWNLOAD, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
                    INSTANCE_ID, REGION)
from worker import (cleanup_downloads, write_activity, checking_for_jobs,
                    get_job_attributes, delete_job_from_queue,
                    process_image, download_and_set, merge_images,
                    name_files)
from worker import (zipfile)


def upload_to_s3(file_location, file_name_zip, input_path, job):
    """Upload processed images to S3."""
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

if __name__ == '__main__':
    checking_for_jobs()
