import os
import transaction
from zope.sqlalchemy import ZopeTransactionExtension
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, UnicodeText, Boolean, DateTime
from datetime import datetime

DBSession = scoped_session(sessionmaker(extension=ZopeTransactionExtension()))
Base = declarative_base()

engine = create_engine(os.environ.get('DATABASE_URL'))
DBSession.configure(bind=engine)
Base.metadata.bind = engine


class RenderCache_Model(Base):
    """
    Model for the already rendered files.
    """
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
        """
        Method adds entry into db given jobid and optional url.
        """
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
        """
        Method updates entry into db given jobid and optional url.
        """
        try:
            DBSession.query(cls).filter(cls.jobid == jobid).update({
                "currentlyrend": currentlyrend, "renderurl": renderurl})
            transaction.commit()
        except:
            print 'Could not update database.'

    @classmethod
    def update_p_url(cls, scene, band1, band2, band3, previewurl):
        """
        Method updates entry into db with preview url.
        """
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
            print 'Could not add the preview URL to the database.'


class UserJob_Model(Base):
    """
    Model for the user job queue. Possible job statuses:
    status_key = {
        0: "In queue",
        1: "Downloading",
        2: "Processing",
        3: "Compressing",
        4: "Uploading to server",
        5: "Done",
        10: "Failed"}
    """
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
        """
        Create a new job in the database.
        """
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
        """
        Set jobstatus for jobid passed in.
        """
        table_key = {1: "status1time",
                     2: "status2time",
                     3: "status3time",
                     4: "status4time",
                     5: "status5time",
                     10: "status10time"}
        try:
            current_time = datetime.utcnow()
            DBSession.query(cls).filter(cls.jobid == int(jobid)).update({
                "jobstatus": status,
                table_key[int(status)]: current_time,
                "lastmodified": current_time
                })
            transaction.commit()
        except:
            print 'Database write failed.'
        # Tell render_cache db we have this image now
        if int(status) == 5:
            try:
                RenderCache_Model.update(jobid, False, url)
            except:
                print 'Could not update the Rendered database.'
