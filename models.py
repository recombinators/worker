from zope.sqlalchemy import ZopeTransactionExtension
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, UnicodeText, Boolean, DateTime
import transaction
import os
from datetime import datetime
import requests

mailgun_key = os.environ['MAILGUN_KEY']
mailgun_url = os.environ['MAILGUN_URL']

DBSession = scoped_session(sessionmaker(extension=ZopeTransactionExtension()))
Base = declarative_base()

engine = create_engine(os.environ.get('DATABASE_URL'))
DBSession.configure(bind=engine)
Base.metadata.bind = engine


class RenderCache_Model(Base):
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
        job = RenderCache_Model(entityid=jobQuery.entityid,
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
            transaction.commit()
        except:
            print 'could not update db'

    @classmethod
    def update_p_url(cls, scene, band1, band2, band3, previewurl):
        '''Method updates entry into db with preview url.'''
        # Convert parameters into correct type
        band1, band2, band3 = int(band1), int(band2), int(band3)
        previewurl = u'{}'.format(previewurl)
        try:
            entry = DBSession.query(cls).filter(cls.entityid == scene,
                                                cls.band1 == band1,
                                                cls.band2 == band2,
                                                cls.band3 == band3).first()
            # update entry if already exists,
            # if there is no existing entry, add it.
            if entry:
                entry.update({"previewurl": previewurl})
                transaction.commit()
            else:
                new = RenderCache_Model(entityid=scene,
                                        band1=band1,
                                        band2=band2,
                                        band3=band3,
                                        previewurl=previewurl
                                        )
                DBSession.add(new)
                transaction.commit()
        except:
            print 'could not add preview url to db'


class UserJob_Model(Base):
    '''
    Model for the user job queue. Possible job statuses:
    status_key = {0: "In queue",
                  1: "Downloading",
                  2: "Processing",
                  3: "Compressing",
                  4: "Uploading to server",
                  5: "Done",
                  10: "Failed"}
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
    rendertype = Column(UnicodeText)
    workerinstanceid = Column(UnicodeText)

    @classmethod
    def new_job(cls,
                entityid=entityid,
                band1=4,
                band2=3,
                band3=2,
                jobstatus=0,
                starttime=datetime.utcnow(),
                rendertype=None
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
                                lastmodified=current_time,
                                rendertype=rendertype
                                )
            session.add(job)
            session.flush()
            session.refresh(job)
            pk = job.jobid
            transaction.commit()
            # could do this or a subtransacation, ie open a transaction at the
            # beginning of this method.
            transaction.begin()
        except:
            return None
        try:
            RenderCache_Model.add(pk, True)
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
            DBSession.query(cls).filter(cls.jobid == int(jobid)).update(
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
                RenderCache_Model.update(jobid, False, url)
            except:
                print 'Could not update Rendered db'
            try:
                cls.email_user(jobid)
            except:
                print 'Email failed'

    @classmethod
    def email_user(cls, jobid):
        """
        If request contains email_address, send email to user with a link to
        the full render zip file.

        """
        job = DBSession.query(cls).filter(cls.jobid == int(jobid)).first()
        email_address = job.email
        if email_address:
            bands = str(job.band1) + str(job.band2) + str(job.band3)
            scene = job.entityid
            full_render = ("http://snapsatcomposites.s3.amazonaws.com/{}_bands"
                           "_{}.zip").format(scene, bands)
            scene_url = 'http://snapsat.org/scene/{}#{}'.format(scene, bands)
            request_url = 'https://api.mailgun.net/v2/{0}/messages'.format(
                mailgun_url)
            requests.post(request_url, auth=('api', mailgun_key),
                          data={
                'from': 'no-reply@snapsat.org',
                'to': email_address,
                'subject': 'Snapsat is rendering your request',
                'text': ("Thank you for using Snapsat.\nAfter we've rendered "
                         "your full composite, it will be available here:\n"
                         "{}\nScene data can be found here:\n {}").format(
                    full_render, scene_url)
            })

    @classmethod
    def set_worker_instance_id(cls, jobid, worker_instance_id):
        """
        Set worker instance id for requested job to track which worker is doing
        the job.
        """

        try:
            DBSession.query(cls).filter(cls.jobid == int(jobid)).update(
                {"workerinstanceid": worker_instance_id})
            transaction.commit()
        except:
            print 'database write failed'
