import pytest
import models
import sqs
import os
from sqlalchemy import create_engine
import unittest
from moto import mock_sqs

# Define AWS credentials
AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
REGION = 'us-west-2'

# Requests are passed into appropriate queues, as defined here.
COMPOSITE_QUEUE = 'composite_test'
PREVIEW_QUEUE = 'preview_test'


####################
# fixtures
@pytest.fixture(scope='session', autouse=True)
def connection(request):
    engine = create_engine('postgresql://postgres@/test_bar')
    models.Base.metadata.create_all(engine)
    connection = engine.connect()
    models.DBSession.registry.clear()
    models.DBSession.configure(bind=connection)
    models.Base.metadata.bind = engine
    request.addfinalizer(models.Base.metadata.drop_all)
    return connection


@pytest.fixture(autouse=True)
def db_session(request, connection):
    from transaction import abort
    trans = connection.begin()
    request.addfinalizer(trans.rollback)
    request.addfinalizer(abort)

    from models import DBSession
    return DBSession


###################
# method tests

@mock_sqs
@pytest.fixture(scope='class')
def conns():
    return sqs.make_SQS_connection(REGION,
                                   AWS_ACCESS_KEY_ID,
                                   AWS_SECRET_ACCESS_KEY)
@mock_sqs
@pytest.fixture(scope='class')
def queue(conns):
    return sqs.get_queue(conns, COMPOSITE_QUEUE)

@pytest.fixture(scope='class')
def job_message():
    body = 'job'
    jobid = 1
    scene = 'LC80470272015101LGN00'
    band1 = 4
    band2 = 3
    band3 = 2

    message = sqs.build_job_message(job_id=jobid,
                                    scene_id=scene,
                                    band_1=band1,
                                    band_2=band2,
                                    band_3=band3)

    return {'body': body,
            'jobid': jobid,
            'scene': scene,
            'band1': band1,
            'band2': band2,
            'band3': band3,
            'message': message}

# class Fake_Job_Class():
#     def __init__(self, message_content, message_attributes):
#         self.message_content = message_content
#         self.message_attributes = message_attributes

# message = {'job_id': {'string_value': 1, 'data_type': 'Number'},
#            'band_2': {'string_value': 3, 'data_type': 'Number'},
#            'band_3': {'string_value': 2, 'data_type': 'Number'},
#            'band_1': {'string_value': 4, 'data_type': 'Number'},
#            'scene_id': {'string_value': 'LC80470272015005LGN00',
#            'data_type': 'String'},
#            'email': {'string_value': 'test@test.com',
#                      'data_type': 'String'}}

# fake_job_for_queue = [Fake_Job_Class("job", message)]
# bad_fake_job = [Fake_Job_Class("job", ['test'])]

def test_make_SQS_connection(conns):
    """Tests make_SQS_connection method."""
    assert COMPOSITE_QUEUE in [i.name for i in conns.get_all_queues()]
    assert PREVIEW_QUEUE in [i.name for i in conns.get_all_queues()]

def test_get_queue(conns):
    """Tests get_queue method."""
    assert COMPOSITE_QUEUE == sqs.get_queue(conns, COMPOSITE_QUEUE).name
    assert PREVIEW_QUEUE == sqs.get_queue(conns, PREVIEW_QUEUE).name

def test_build_result_message_message(job_message):
    assert job_message['body'] == job_message['message']['body']

def test_build_result_message_attributes_jobid(job_message):
    assert 'Number' == job_message['message']['attributes']['job_id']['data_type']
    assert job_message['jobid'] == job_message['message']['attributes']['job_id']['string_value']

def test_build_result_message_attributes_sceneid(job_message):
    assert 'String' == job_message['message']['attributes']['scene_id']['data_type']
    assert job_message['scene'] == job_message['message']['attributes']['scene_id']['string_value']

def test_build_result_message_attributes_band1(job_message):
    assert 'Number' == job_message['message']['attributes']['band_1']['data_type']
    assert job_message['band1'] == job_message['message']['attributes']['band_1']['string_value']

def test_build_result_message_attributes_band2(job_message):
    assert 'Number' == job_message['message']['attributes']['band_2']['data_type']
    assert job_message['band2'] == job_message['message']['attributes']['band_2']['string_value']

def test_build_result_message_attributes_band3(job_message):
    assert 'Number' == job_message['message']['attributes']['band_3']['data_type']
    assert job_message['band3'] == job_message['message']['attributes']['band_3']['string_value']

def test_send_message(conns, queue, job_message):
    sqs.send_message(conns,
                     queue,
                     job_message['message']['body'],
                     job_message['message']['attributes'])
    assert queue.count() == 1

def test_queue_size(queue):
    assert sqs.queue_size(queue) == 1

def test_get_message(queue):
    message = sqs.get_message(queue, visibility_timeout=0)
    assert len(message) == 1

def test_get_attributes_jobid(job_message, queue):
    message = sqs.get_message(queue, visibility_timeout=0)
    attrs = sqs.get_attributes(message)
    assert job_message['jobid'] == int(attrs['job_id'])

def test_get_attributes_sceneid(job_message, queue):
    message = sqs.get_message(queue, visibility_timeout=0)
    attrs = sqs.get_attributes(message)
    assert job_message['scene'] == attrs['scene_id']

def test_get_attributes_band1(job_message, queue):
    message = sqs.get_message(queue, visibility_timeout=0)
    attrs = sqs.get_attributes(message)
    assert job_message['band1'] == int(attrs['band_1'])

def test_get_attributes_band2(job_message, queue):
    message = sqs.get_message(queue, visibility_timeout=0)
    attrs = sqs.get_attributes(message)
    assert job_message['band2'] == int(attrs['band_2'])

def test_get_attributes_band3(job_message, queue):
    message = sqs.get_message(queue, visibility_timeout=0)
    attrs = sqs.get_attributes(message)
    assert job_message['band3'] == int(attrs['band_3'])

def test_delete_message_from_handle(conns, queue):
    message = sqs.get_message(queue, visibility_timeout=0)
    sqs.delete_message_from_handle(conns, queue, message[0])
    assert queue.count() == 0
