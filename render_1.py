import sys
sys.path.append('landsat-util/landsat')
from downloader import Downloader
from image import Process
import os
import boto
from boto.s3.connection import S3Connection
from boto.s3.key import Key
import zipfile


# sceneID = ['LC80030172015001LGN00']
sceneID = ['LC80460272015014LGN00']
bands = [4, 3, 2]

path = '/home/ubuntu/dl'
# sceneID='LC80030172015001LGN00'

AWS_ACCESS_KEY_ID = os.environ('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.environ('AWS_SECRET_ACCESS_KEY')


def process():
    """Given bands and sceneID, download, image process, zip & upload to S3."""
    b = Downloader(verbose=True, download_dir=path)
    b.download(sceneID, bands)
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
    file_location = os.path.join('/home/ubuntu/landsat_worker', file_name_zip)
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
    
    # generates url that works for 1 hour
    # plans_url = plans_key.generate_url(3600, query_auth=True, force_http=True)

process()
