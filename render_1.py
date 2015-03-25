import sys
sys.path.append('landsat-util/landsat')
from downloader import Downloader
from image import Process
import os
import boto
from boto.s3.connection import S3Connection
from boto.s3.key import Key
import zipfile
import requests
import json
import sqs


path = '/Users/chatzis/projects/landsatproject/landsat_worker/dl'

AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
LANDSAT_JOBS_QUEUE = 'landsat_jobs_queue'


def main():
    checking_for_jobs()


def checking_for_jobs():
    '''Poll jobs queue for jobs.'''
    conn = sqs.make_connection(aws_access_key_id=AWS_ACCESS_KEY_ID,
                               aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    jobs_queue = sqs.get_queue(LANDSAT_JOBS_QUEUE, conn)
    while True:
        job_message = sqs.get_message(jobs_queue)
        if job_message:
            job_attributes = sqs.get_attributes(job_message)
            success = process(job_attributes)
        if job_message and success:
            sqs.delete_message_from_queue(job_message, jobs_queue)


def process(job):
    '''Given bands and sceneID, download, image process, zip & upload to S3.'''
    b = Downloader(verbose=True, download_dir=path)
    scene_id = [str(job['scene_id'])]
    bands = [job['band_1'], job['band_2'], job['band_3']]
    b.download(scene_id, bands)
    input_path = os.path.join(path, scene_id[0])

    c = Process(input_path, bands=bands, dst_path=path, verbose=True)
    c.run(pansharpen=False)

    band_output = ''

    for i in bands:
        band_output = '{}{}'.format(band_output, i)
    file_name = '{}_bands_{}.TIF'.format(scene_id[0], band_output)
    file_location = os.path.join(input_path, file_name)

    # zip file, maintain location
    file_name_zip = '{}_bands_{}.zip'.format(scene_id[0], band_output)
    path_to_zip = os.path.join(input_path, file_name_zip)
    with zipfile.ZipFile(path_to_zip, 'w', zipfile.ZIP_DEFLATED) as myzip:
        myzip.write(file_location)

    # upload to s3
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
    send_post_request(out)
    return out
    
    # generates url that works for 1 hour
    # plans_url = plans_key.generate_url(3600, query_auth=True, force_http=True)


def send_post_request(pic_url):
    """Send post request to pyramid app, to notify completion."""
    payload = {'url': pic_url}
    post_url = "http://landsat.club/done/"
    r = requests.post(post_url, data=json.dumps(payload))
    print "post request sent"
    return True

if __name__ == '__main__':
    main()
