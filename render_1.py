import sys
sys.path.append('landsat-util/landsat')
from downloader import Downloader
from image import Process
import os
import boto
from boto.s3.connection import S3Connection
from boto.s3.key import Key


# lat = 38.9004204
# lon = -77.0237117
# a = Search()
# output = a.search(lat=lat, lon=lon, limit=10)
# print 'output length: {}'.format(len(output))
# import pdb; pdb.set_trace()
# # sceneID = output['results'][0]['sceneID']
# # sceneID = []
# # for i in output['results']:
# #     sceneID.append(str(i['sceneID']))
# sceneID = [str(i['sceneID']) for i in output['results']]
# print sceneID

sceneID = ['LC80030172015001LGN00']
bands = [4, 3, 2]

path = '/Users/mark/projects/landsat_worker/dl2'
# sceneID='LC80030172015001LGN00'
b = Downloader(verbose=True, download_dir=path)
b.download(sceneID, bands)
input_path = os.path.join(path, sceneID[0])
dest_path = input_path

# c = Process(input_path, bands=bands, dst_path=path, verbose=True)
# c.run(pansharpen=False)

band_output = ''

for i in bands:
    band_output = '{}{}'.format(band_output, i)
file_name = '{}_bands_{}.TIF'.format(sceneID[0], band_output)
file_location = os.path.join(input_path, file_name)

file_name = 'test.png'
file_location = '/Users/mark/Desktop/test.png'
conne = boto.connect_s3()
b = conne.get_bucket('landsatproject')
k = Key(b)
k.key = 'test'
k.set_contents_from_filename(file_location)
k.get_contents_to_filename(file_location)
hello = b.get_key('test')
# set to be public
hello.set_canned_acl('public-read')
hello.generate_url(0, query_auth=False, force_http=True)


# generates url that works for 1 hour
# plans_url = plans_key.generate_url(3600, query_auth=True, force_http=True)
