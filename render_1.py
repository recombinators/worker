import sys
sys.path.append('landsat-util/landsat')
from downloader import Downloader
from image import Process
import os
import boto
from boto.s3.key import Key
import zipfile
import requests
from sqs import (make_SQS_connection, get_queue, get_message, get_attributes,
                 delete_message_from_handle,)
from shutil import rmtree
from datetime import datetime
from zope.sqlalchemy import ZopeTransactionExtension
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, UnicodeText, Boolean, DateTime
import transaction

DBSession = scoped_session(sessionmaker(extension=ZopeTransactionExtension()))
Base = declarative_base()

engine = create_engine(os.environ.get('DATABASE_URL'))
DBSession.configure(bind=engine)
Base.metadata.bind = engine


class Rendered_Model(Base):
    '''Model for the already rendered files'''
    __tablename__ = 'render_cache'
    id = Column(Integer, primary_key=True)
    jobid = Column(Integer)
    entityid = Column(UnicodeText)
    band1 = Column(Integer)
    band2 = Column(Integer)
    band3 = Column(Integer)
    previewurl = Column(UnicodeText)
    renderurl = Column(UnicodeText)
    rendercount = Column(Integer, default=0)
    currentlyrend = Column(Boolean)

    @classmethod
    def add(cls, jobid, currentlyrend):
        '''Method adds entry into db given jobid and optional url.'''
        jobQuery = DBSession.query(UserJob_Model).get(jobid)
        job = Rendered_Model(entityid=jobQuery.entityid,
                             jobid=jobid,
                             band1=jobQuery.band1,
                             band2=jobQuery.band2,
                             band3=jobQuery.band3,
                             currentlyrend=currentlyrend)
        DBSession.add(job)
        transaction.commit()

    @classmethod
    def update(cls, jobid, currentlyrend, renderurl):
        '''Method updates entry into db given jobid and optional url.'''
        try:
            DBSession.query(cls).filter(cls.jobid == jobid).update({
                "currentlyrend": currentlyrend, "renderurl": renderurl})
        except:
            print 'could not update db'


class UserJob_Model(Base):
    '''Model for the user job queue. Possible job statuses:
    0 - Created
    1 - Queued
    2 - Processing
    3 - Done (Failed)
    4 - Done (Success)
    '''

    __tablename__ = 'user_job'
    jobid = Column(Integer, primary_key=True)
    entityid = Column(UnicodeText)
    userip = Column(UnicodeText)
    email = Column(UnicodeText)
    band1 = Column(Integer)
    band2 = Column(Integer)
    band3 = Column(Integer)
    jobstatus = Column(Integer, nullable=False)
    starttime = Column(DateTime, nullable=False)
    lastmodified = Column(DateTime, nullable=False)
    status1time = Column(DateTime)
    status2time = Column(DateTime)
    status3time = Column(DateTime)
    status4time = Column(DateTime)
    status5time = Column(DateTime)
    status10time = Column(DateTime)

    @classmethod
    def new_job(cls,
                entityid=entityid,
                band1=4,
                band2=3,
                band3=2,
                jobstatus=0,
                starttime=datetime.utcnow(),
                ):
        '''Create new job in db.'''
        try:
            session = DBSession
            current_time = datetime.utcnow()
            job = UserJob_Model(entityid=entityid,
                                band1=band1,
                                band2=band2,
                                band3=band3,
                                jobstatus=0,
                                starttime=current_time,
                                lastmodified=current_time
                                )
            session.add(job)
            session.flush()
            session.refresh(job)
            pk = job.jobid
            transaction.commit()
            transaction.begin() # could do this or a subtransacation, ie open a transaction at the beginning of this method.
        except:
            return None
        try:
            Rendered_Model.add(pk, True)
        except:
            print 'Could not add job to rendered db'
        return pk

    @classmethod
    def set_job_status(cls, jobid, status, url=None):
        '''Set jobstatus for jobid passed in.'''
        table_key = {1: "status1time",
                     2: "status2time",
                     3: "status3time",
                     4: "status4time",
                     5: "status5time",
                     10: "status10time"}
        try:
            current_time = datetime.utcnow()
            DBSession.query(cls).filter(cls.jobid == jobid).update(
                                    {"jobstatus": status,
                                     table_key[int(status)]: current_time,
                                     "lastmodified": current_time
                                     })
            transaction.commit()
        except:
            print 'database write failed'
        # Tell render_cache db we have this image now
        if int(status) == 5:
            try:
                Rendered_Model.update(jobid, False, url)
            except:
                print 'Could not update Rendered db'

os.getcwd()
path_download = os.getcwd() + '/download'
path_error_log = os.getcwd() + '/logs' + '/error_log.txt'
path_activity_log = os.getcwd() + '/logs' + '/activity_log.txt'

AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
JOBS_QUEUE = 'landsat_jobs_queue'
REGION = 'us-west-2'


def cleanup_downloads(folder_path):
    '''Clean up download folder if process fails. Return True if download folder
       empty'''
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
    '''Write to error log.'''
    fo = open(path_activity_log, 'a')
    fo.write(message + '\n')
    fo.close()


def write_error(message):
    '''Write to error log.'''
    fo = open(path_activity_log, 'a')
    fo.write(message + '\n')
    fo.close()


def main():
    '''Main.'''
    checking_for_jobs()


def checking_for_jobs():
    '''Poll jobs queue for jobs.'''
    SQSconn = make_SQS_connection(REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    write_activity('[{}] {}'.format(datetime.utcnow(), SQSconn))
    jobs_queue = get_queue(SQSconn, JOBS_QUEUE)
    write_activity('[{}] {}'.format(datetime.utcnow(), jobs_queue))
    while True:
        job_message = get_message(jobs_queue)
        if job_message:
            try:
                job_attributes = get_attributes(job_message[0])
                write_activity('[{}] {}'.format(datetime.utcnow(),
                                                job_attributes))
            except Exception as e:
                write_activity('[{}] Attribute retrieval fail because {}'
                               .format(datetime.utcnow(), e.message))
                write_error('[{}] Attribute retrieval fail because {}'
                            .format(datetime.utcnow(), e.message))

            try:
                del_status = delete_message_from_handle(SQSconn,
                                                        jobs_queue,
                                                        job_message[0])
                write_activity('[{}] Delete success = {}'
                               .format(datetime.utcnow(), del_status))
            except Exception as e:
                write_activity('[{}] Delete success = {}'
                               .format(datetime.utcnow(), del_status))
                write_activity('[{}] Delete message fail because {}'
                               .format(datetime.utcnow(), e.message))
                write_error('[{}] Delete message fail because {}'
                            .format(datetime.utcnow(), e.message))

            # Process full res images
            try:
                proc_status = process(job_attributes)
                write_activity('[{}] Job Process success = {}'
                               .format(datetime.utcnow(),
                                       proc_status))
            except Exception as e:
                # If processing fails, send message to pyramid to update db
                write_activity('[{}] Job process success = {}'
                               .format(datetime.utcnow(), False))
                write_activity('[{}] Job process fail because {}'
                               .format(datetime.utcnow(), e.message))
                write_error('[{}] Job process fail because {}'
                            .format(datetime.utcnow(), e.message))
                cleanup_status = cleanup_downloads(path_download)
                write_activity('[{}] Cleanup downloads success = {}'
                               .format(datetime.utcnow(),
                                       cleanup_status))
                write_error('[{}] Cleanup downloads success = {}'
                            .format(datetime.utcnow(), cleanup_status))
                UserJob_Model.set_job_status(job_attributes['job_id'], 10)


def process(job):
    '''Given bands and sceneID, download, image process, zip & upload to S3.'''

    UserJob_Model.set_job_status(job['job_id'], 1)
    b = Downloader(verbose=True, download_dir=path_download)
    scene_id = [str(job['scene_id'])]
    bands = [job['band_1'], job['band_2'], job['band_3']]
    b.download(scene_id, bands)
    input_path = os.path.join(path_download, scene_id[0])

    UserJob_Model.set_job_status(job['job_id'], 2)
    c = Process(input_path, bands=bands, dst_path=path_download, verbose=True)
    c.run(pansharpen=False)

    band_output = ''

    for i in bands:
        band_output = '{}{}'.format(band_output, i)
    file_name = '{}_bands_{}.TIF'.format(scene_id[0], band_output)
    file_location = os.path.join(input_path, file_name)

    # zip file, maintain location
    print 'Zipping file'
    UserJob_Model.set_job_status(job['job_id'], 3)
    file_name_zip = '{}_bands_{}.zip'.format(scene_id[0], band_output)
    path_to_zip = os.path.join(input_path, file_name_zip)
    with zipfile.ZipFile(path_to_zip, 'w', zipfile.ZIP_DEFLATED) as myzip:
        myzip.write(file_location)

    # upload to s3
    print 'Uploading to S3'
    UserJob_Model.set_job_status(job['job_id'], 4)

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
    UserJob_Model.set_job_status(job['job_id'], 5, out)

    # delete files
    try:
        rmtree(input_path)           # band images and composite
    except OSError:
        print input_path
        print 'error deleting files'

    return True

if __name__ == '__main__':
    main()
