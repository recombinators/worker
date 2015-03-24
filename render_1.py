import sys
sys.path.append('landsat-util/landsat')
from downloader import Downloader
from image import Process
import os
import boto
from boto.s3.connection import S3Connection
from boto.s3.key import Key
import zipfile
import sqs


# sceneID = ['LC80030172015001LGN00']
sceneID = ['LC80460272015014LGN00']
bands = [4, 3, 2]

path = '~/dl'
# sceneID='LC80030172015001LGN00'

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
        job_attributes = sqs.get_attributes(job_message)
        process(job_attributes)
        if job_message:
            sqs.delete_message_from_queue(job_message, jobs_queue)


def process(job):
    '''Given bands and sceneID, download, image process, zip & upload to S3.'''
    b = Downloader(verbose=True, download_dir=path)
    # import pdb; pdb.set_trace()
    b.download([str(job['scene_id'])],
               [job['band_1'], job['band_2'], job['band_3']])
    input_path = os.path.join(path, sceneID[0])

    c = Process(input_path, bands=bands, dst_path=path, verbose=True)
    c.run(pansharpen=False)

    band_output = ''

    for i in bands:
        band_output = '{}{}'.format(band_output, i)
    file_name = '{}_bands_{}.TIF'.format(sceneID[0], band_output)
    file_location = os.path.join(input_path, file_name)

    # zip file, maintain location
    file_name_zip = '{}_bands_{}.zip'.format(sceneID[0], band_output)
    zf = zipfile.ZipFile(file_name_zip, 'w', zipfile.ZIP_DEFLATED)
    zf.write(file_location)
    zf.close()

    # upload to s3
    file_location = os.path.join('~/landsat_worker', file_name_zip)
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
    return out

if __name__ == '__main__':
    main()
