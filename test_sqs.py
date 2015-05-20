import pytest
import models
import sqs
import os

# Define AWS credentials
AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
REGION = 'us-west-2'

# Requests are passed into appropriate queues, as defined here.
COMPOSITE_QUEUE = 'composite_test'
PREVIEW_QUEUE = 'preview_test'


####################
# fixtures
@pytest.fixture(scope='module')
def conn():
    return sqs.make_SQS_connection(REGION,
                                   AWS_ACCESS_KEY_ID,
                                   AWS_SECRET_ACCESS_KEY)


@pytest.fixture(scope='module')
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


@pytest.fixture(scope='module')
def queue(conn):
    return sqs.get_queue(conn, COMPOSITE_QUEUE)


###################
# method tests
def test_make_SQS_connection(conn):
    """Tests make_SQS_connection method."""
    assert COMPOSITE_QUEUE in [i.name for i in conn.get_all_queues()]
    assert PREVIEW_QUEUE in [i.name for i in conn.get_all_queues()]


def test_get_queue(conn):
    """Tests get_queue method."""
    assert COMPOSITE_QUEUE == sqs.get_queue(conn, COMPOSITE_QUEUE).name
    assert PREVIEW_QUEUE == sqs.get_queue(conn, PREVIEW_QUEUE).name


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


def test_send_message(conn, queue, job_message):
    sqs.send_message(conn,
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


def test_delete_message_from_handle(conn, queue):
    message = sqs.get_message(queue, visibility_timeout=0)
    sqs.delete_message_from_handle(conn, queue, message[0])
    assert queue.count() == 0
