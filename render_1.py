import sys
sys.path.append('landsat-util/landsat')
from downloader import Downloader
from image import Process
import os
import boto
from boto.s3.key import Key
import zipfile
import requests
from sqs import (make_connection, get_queue, get_message, get_attributes,
                 delete_message_from_handle,)
from shutil import rmtree


path = '/home/ubuntu/dl'

AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
JOBS_QUEUE = 'landsat_jobs_queue'
REGION = 'us-west-2'


def send_post_request(pk, status=10, pic_url=None):
    """Send post request to pyramid app, to notify completion."""
    payload = {'url': pic_url, 'pk': pk, 'status': status}
    post_url = "http://develop.landsat.club/done"
    requests.post(post_url, data=payload)
    print "post request sent to {}".format(post_url)
    return True


def main():
    checking_for_jobs()


def checking_for_jobs():
    '''Poll jobs queue for jobs.'''
    SQSconn = make_connection(REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    jobs_queue = get_queue(SQSconn, JOBS_QUEUE)
    while True:
        job_message = get_message(jobs_queue)
        if job_message:
            job_attributes = get_attributes(job_message)
            delete_message_from_handle(SQSconn, jobs_queue, job_message[0])
            try:
                process(job_attributes)
            except:
                # If processing fails, send message to pyramid to update db
                send_post_request(job_attributes['pk'], 10)


def process(job):
    '''Given bands and sceneID, download, image process, zip & upload to S3.'''

    send_post_request(job['pk'], 1)
    b = Downloader(verbose=True, download_dir=path)
    scene_id = [str(job['scene_id'])]
    bands = [job['band_1'], job['band_2'], job['band_3']]
    b.download(scene_id, bands)
    input_path = os.path.join(path, scene_id[0])

    send_post_request(job['pk'], 2)
    c = Process(input_path, bands=bands, dst_path=path, verbose=True)
    c.run(pansharpen=False)

    band_output = ''

    for i in bands:
        band_output = '{}{}'.format(band_output, i)
    file_name = '{}_bands_{}.TIF'.format(scene_id[0], band_output)
    file_location = os.path.join(input_path, file_name)

    # zip file, maintain location
    print 'Zipping file'
    send_post_request(job['pk'], 3)
    file_name_zip = '{}_bands_{}.zip'.format(scene_id[0], band_output)
    path_to_zip = os.path.join(input_path, file_name_zip)
    with zipfile.ZipFile(path_to_zip, 'w', zipfile.ZIP_DEFLATED) as myzip:
        myzip.write(file_location)

    # upload to s3
    print 'Uploading to S3'
    send_post_request(job['pk'], 4)

    file_location = os.path.join(input_path, file_name_zip)
    conne = boto.connect_s3(aws_access_key_id=AWS_ACCESS_KEY_ID,
                            aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    b = conne.get_bucket('landsatproject')
    k = Key(b)
    k.key = file_name_zip
    k.set_contents_from_filename(file_location)
    k.get_contents_to_filename(file_location)
    hello = b.get_key(file_name_zip)
    # make public
    hello.set_canned_acl('public-read')

    out = hello.generate_url(0, query_auth=False, force_http=True)
    print out
    send_post_request(job['pk'], 5, out)

    # delete files
    try:
        rmtree(input_path)           # band images and composite
        os.remove(file_location)        # zip file
    except OSError:
        print 'error deleting files'
        pass

    return True


    # generates url that works for 1 hour
    # plans_url = plans_key.generate_url(3600, query_auth=True, force_http=True)

if __name__ == '__main__':
    main()
