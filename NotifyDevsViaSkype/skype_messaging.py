#   2013-04-02  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   added socket_timeout 

import pika
import simplejson as json

def send_message(message, skype_id=None, group_chat=None):
    """
    >>> send_message('ping', skype_id='locsunglao')
    """
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='10.9.8.7', socket_timeout=2))
    channel = connection.channel()
    channel.queue_declare(queue='fr5w_rmq')

    msg = dict(message=message)
    if skype_id == None:
        msg['group_chat'] = group_chat
    else:
        msg['skype_id'] = skype_id

    content = json.dumps(msg)

    channel.basic_publish(exchange='', routing_key='fr5w_rmq', body=content)

    connection.close()


if __name__ == "__main__":
    import doctest
    doctest.testmod()

