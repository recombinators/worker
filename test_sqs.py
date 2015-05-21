import pytest
import sqs
from moto import mock_sqs
import boto

# Define AWS credentials
AWS_ACCESS_KEY_ID = 'the_key'
AWS_SECRET_ACCESS_KEY = 'the_secret'
REGION = 'us-west-2'

# Requests are passed into appropriate queues, as defined here.
COMPOSITE_QUEUE = 'composite_test'
PREVIEW_QUEUE = 'preview_test'


###################
# method tests

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


# @mock_sqs
# @pytest.fixture(scope='function')
# def conns():
#     conns = boto.sqs.connect_to_region(REGION,
#                                        aws_access_key_id=AWS_ACCESS_KEY_ID,
#                                        aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
#     return conns


# @mock_sqs
# @pytest.fixture(scope='function')
# def queue():
#     conns = boto.sqs.connect_to_region(REGION,
#                                        aws_access_key_id=AWS_ACCESS_KEY_ID,
#                                        aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
#     queue = conns.create_queue(COMPOSITE_QUEUE)
#     return conns, queue


# @mock_sqs
# @pytest.fixture(scope='function')
# def qm(request, queue, job_message):
#     queue[0].send_message(queue=queue[1],
#                           message_content=job_message['message']['body'],
#                           message_attributes=job_message['message']['attributes'])

#     def fin():
#         queue[0].purge_queue(queue[1])

#     request.addfinalizer(fin)
#     return queue[1]


@mock_sqs
def test_make_SQS_connection():
    """Tests make_SQS_connection method."""
    conns = sqs.make_SQS_connection(REGION,
                                    AWS_ACCESS_KEY_ID,
                                    AWS_SECRET_ACCESS_KEY)
    assert conns.server_name() == 'us-west-2.queue.amazonaws.com'


@mock_sqs
def test_get_queue():
    """Tests get_queue method."""
    conns = boto.sqs.connect_to_region(REGION,
                                       aws_access_key_id=AWS_ACCESS_KEY_ID,
                                       aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    conns.create_queue(COMPOSITE_QUEUE)
    assert COMPOSITE_QUEUE == sqs.get_queue(conns, COMPOSITE_QUEUE).name


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


@mock_sqs
def test_send_message(job_message):
    conns = boto.sqs.connect_to_region(REGION,
                                       aws_access_key_id=AWS_ACCESS_KEY_ID,
                                       aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    queue = conns.create_queue(COMPOSITE_QUEUE)
    start = queue.count()
    sqs.send_message(conns,
                     queue,
                     job_message['message']['body'],
                     job_message['message']['attributes'])
    assert queue.count() - start == 1


@mock_sqs
def test_queue_size():
    conns = boto.sqs.connect_to_region(REGION,
                                       aws_access_key_id=AWS_ACCESS_KEY_ID,
                                       aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    queue = conns.create_queue(COMPOSITE_QUEUE)
    assert sqs.queue_size(queue) == queue.count()


@mock_sqs
def test_get_message(job_message):
    conns = boto.sqs.connect_to_region(REGION,
                                       aws_access_key_id=AWS_ACCESS_KEY_ID,
                                       aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    queue = conns.create_queue(COMPOSITE_QUEUE)
    conns.send_message(queue=queue,
                       message_content=job_message['message']['body'],
                       message_attributes=job_message['message']['attributes'])
    message = queue.get_messages()
    assert len(message) == 1


@mock_sqs
def test_get_attributes_jobid(job_message):
    conns = boto.sqs.connect_to_region(REGION,
                                       aws_access_key_id=AWS_ACCESS_KEY_ID,
                                       aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    queue = conns.create_queue(COMPOSITE_QUEUE)
    conns.send_message(queue=queue,
                       message_content=job_message['message']['body'],
                       message_attributes=job_message['message']['attributes'])
    message = queue.get_messages()
    attrs = sqs.get_attributes(message[0])
    assert job_message['jobid'] == int(attrs['job_id'])


@mock_sqs
def test_get_attributes_sceneid(job_message):
    conns = boto.sqs.connect_to_region(REGION,
                                       aws_access_key_id=AWS_ACCESS_KEY_ID,
                                       aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    queue = conns.create_queue(COMPOSITE_QUEUE)
    conns.send_message(queue=queue,
                       message_content=job_message['message']['body'],
                       message_attributes=job_message['message']['attributes'])
    message = queue.get_messages()
    attrs = sqs.get_attributes(message[0])
    assert job_message['scene'] == attrs['scene_id']


@mock_sqs
def test_get_attributes_band1(job_message):
    conns = boto.sqs.connect_to_region(REGION,
                                       aws_access_key_id=AWS_ACCESS_KEY_ID,
                                       aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    queue = conns.create_queue(COMPOSITE_QUEUE)
    conns.send_message(queue=queue,
                       message_content=job_message['message']['body'],
                       message_attributes=job_message['message']['attributes'])
    message = queue.get_messages()
    attrs = sqs.get_attributes(message[0])
    assert job_message['band1'] == int(attrs['band_1'])


@mock_sqs
def test_get_attributes_band2(job_message):
    conns = boto.sqs.connect_to_region(REGION,
                                       aws_access_key_id=AWS_ACCESS_KEY_ID,
                                       aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    queue = conns.create_queue(COMPOSITE_QUEUE)
    conns.send_message(queue=queue,
                       message_content=job_message['message']['body'],
                       message_attributes=job_message['message']['attributes'])
    message = queue.get_messages()
    attrs = sqs.get_attributes(message[0])
    assert job_message['band2'] == int(attrs['band_2'])


@mock_sqs
def test_get_attributes_band3(job_message):
    conns = boto.sqs.connect_to_region(REGION,
                                       aws_access_key_id=AWS_ACCESS_KEY_ID,
                                       aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    queue = conns.create_queue(COMPOSITE_QUEUE)
    conns.send_message(queue=queue,
                       message_content=job_message['message']['body'],
                       message_attributes=job_message['message']['attributes'])
    message = queue.get_messages()
    attrs = sqs.get_attributes(message[0])
    assert job_message['band3'] == int(attrs['band_3'])


@mock_sqs
def test_delete_message_from_handle(job_message):
    conns = boto.sqs.connect_to_region(REGION,
                                       aws_access_key_id=AWS_ACCESS_KEY_ID,
                                       aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    queue = conns.create_queue(COMPOSITE_QUEUE)
    conns.send_message(queue=queue,
                       message_content=job_message['message']['body'],
                       message_attributes=job_message['message']['attributes'])
    start = queue.count()
    message = queue.get_messages()
    sqs.delete_message_from_handle(conns, queue, message[0])
    assert start - queue.count() == 1
