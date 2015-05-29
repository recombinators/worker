import os
import sys
import boto
import subprocess
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

from worker import PREVIEW_QUEUE as JOBS_QUEUE
from worker import PREVIEW_BUCKET as BUCKET
from worker import (PATH_DOWNLOAD, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
                    REGION, INSTANCE_ID)
from worker import (cleanup_downloads, write_activity, checking_for_jobs,
                    get_job_attributes, delete_job_from_queue,
                    process_image, download_and_set, merge_images,
                    name_files)
from worker import (resize_bands, remove_and_rename, tif_to_png)


def upload_to_s3(file_location, file_png, job):
    """Upload the processed file to S3, update job database"""
    try:
        print 'Uploading to S3'
        UserJob_Model.set_job_status(job['job_id'], 4)
        conne = boto.connect_s3(aws_access_key_id=AWS_ACCESS_KEY_ID,
                                aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
        b = conne.get_bucket(BUCKET)
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
