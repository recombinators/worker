import sys
sys.path.append('landsat-util/landsat')
from landsat.downloader import Downloader
from image import Process
import os
import boto
from boto.s3.key import Key
import zipfile
from sqs import (make_SQS_connection, get_queue, get_message, get_attributes,
                 delete_message_from_handle,)
from shutil import rmtree
from datetime import datetime
from models import (UserJob_Model)
from boto import utils


os.getcwd()
PATH_DOWNLOAD = os.getcwd() + '/download'
PATH_ERROR_LOG = os.getcwd() + '/logs' + '/error_log.txt'
PATH_ACTIVITY_LOG = os.getcwd() + '/logs' + '/activity_log.txt'

AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
JOBS_QUEUE = 'snapsat_composite_queue'
REGION = 'us-west-2'

INSTANCE_METADATA = utils.get_instance_metadata(timeout=0.5, num_retries=1)
INSTANCE_ID = INSTANCE_METADATA['instance-id']


def cleanup_downloads(folder_path):
    """Clean up download folder if process fails. Return True if download folder
       empty"""
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


def main():
    """Main."""
    checking_for_jobs()


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
                write_activity('{}'.format(job_attributes))
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
                write_activity('Job Process success = {}'
                               .format(proc_status))
            except Exception as e:
                # If processing fails, send message to pyramid to update db
                write_activity('Job process success = {}'.format(False))
                write_activity('Job process fail because {}'.format(e.message))
                write_error('Job process fail because {}'.format(e.message))
                write_activity('Job proceess traceback: {}'
                               .format(sys.exc_info()))
                write_error('Job process traceback: {}'.format(sys.exc_info()))
                cleanup_status = cleanup_downloads(PATH_DOWNLOAD)
                write_activity('Cleanup downloads success = {}'
                               .format(cleanup_status))
                write_error('Cleanup downloads success = {}'
                            .format(cleanup_status))
                UserJob_Model.set_job_status(job_attributes['job_id'], 10)

# begin process() breakdown here:


def download_and_set(job, PATH_DOWNLOAD):
    """Download the image file"""
    UserJob_Model.set_job_status(job['job_id'], 1)
    b = Downloader(verbose=False, download_dir=PATH_DOWNLOAD)
    scene_id = str(job['scene_id'])
    bands = [job['band_1'], job['band_2'], job['band_3']]
    b.download([scene_id], bands)
    input_path = os.path.join(PATH_DOWNLOAD, scene_id)
    return input_path, bands, scene_id


def process_image(job, input_path, bands, PATH_DOWNLOAD, scene_id):
    UserJob_Model.set_job_status(job['job_id'], 2)
    c = Process(input_path, bands=bands, dst_path=PATH_DOWNLOAD, verbose=True)
    c.run(pansharpen=False)

    band_output = ''

    for band in bands:
        band_output = '{}{}'.format(band_output, band)
    file_name = '{}_bands_{}.TIF'.format(scene_id, band_output)
    file_location = os.path.join(input_path, file_name)
    return band_output, file_location


def zip_file(job, band_output, scene_id, input_path, file_location):
    print 'Zipping file'
    UserJob_Model.set_job_status(job['job_id'], 3)
    file_name_zip = '{}_bands_{}.zip'.format(scene_id, band_output)
    file_name = '{}_bands_{}.TIF'.format(scene_id, band_output)
    path_to_zip = os.path.join(input_path, file_name_zip)
    with zipfile.ZipFile(path_to_zip, 'w', zipfile.ZIP_DEFLATED) as myzip:
        myzip.write(file_location, arcname=file_name)
    return file_name_zip


def upload_to_s3(file_location, file_name_zip, input_path, job):
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
    UserJob_Model.set_job_status(job['job_id'], INSTANCE_ID)

    # download and set vars
    input_path, bands, scene_id = download_and_set(job, PATH_DOWNLOAD)

    # process image
    band_output, file_location = process_image(
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
    main()
