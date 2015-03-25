import requests


def send_post_request(pic_url='http://test.com', pk='15'):
    """Send post request to pyramid app, to notify completion."""
    payload = {'url': pic_url, 'pk': pk}
    post_url = "http://localhost:8000/done"
    r = requests.post(post_url, data=payload)
    print "post request sent"
    return True

if __name__ == '__main__':
    send_post_request()
