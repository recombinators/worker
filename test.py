import requests



def send_post_request(job_id, status=10, pic_url=None):
    """Send post request to pyramid app, to notify completion."""
    payload = {'url': pic_url, 'job_id': job_id, 'status': status}
    post_url = "http://localhost:8000/done"
    requests.post(post_url, data=payload)
    # print "post request sent to {}".format(post_url)
    if status == 5:
        print 'job_id: {} done.'.format(job_id)
    return True

if __name__ == '__main__':
    send_post_request(120, 5, 'http://develop.landsat.club/scene/LC80470272014274LGN00')
