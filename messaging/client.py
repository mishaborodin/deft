__author__ = 'Dmitry Golubkov'

import ssl
import stomp
import socket
from deftcore.settings import VOMS_CERT_FILE_PATH, VOMS_KEY_FILE_PATH, MessagingConfig
from deftcore.log import Logger
from .listener import Listener

logger = Logger.get()


class Client(object):
    _id = 0

    def __init__(self, hostname, port, no_db_log=False):
        self.id = Client._id
        Client._id += 1

        cert_file = VOMS_CERT_FILE_PATH
        key_file = VOMS_KEY_FILE_PATH

        self.connection = stomp.Connection(
            host_and_ports=[(hostname, port)],
            use_ssl=True, ssl_version=ssl.PROTOCOL_TLSv1,
            ssl_key_file=key_file,
            ssl_cert_file=cert_file)

        self.connection.set_listener('messaging_listener', Listener(no_db_log=no_db_log))

    def connect(self):
        self.connection.start()
        self.connection.connect(wait=True)
        self.connection.subscribe(destination=MessagingConfig.QUEUE, id=str(self.id), ack='auto')
        logger.info('id={0}'.format(self.id))

    def disconnect(self):
        self.connection.disconnect()
