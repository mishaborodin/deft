__author__ = 'Dmitry Golubkov'

import socket
from deftcore.log import Logger
from deftcore.settings import MessagingConfig
from .client import Client

logger = Logger.get()


class Manager(object):
    def __init__(self, no_db_log=False):
        self.client_list = list()
        self._no_db_log = no_db_log

    def start(self):
        for info in socket.getaddrinfo(MessagingConfig.HOSTNAME, MessagingConfig.PORT, 0, 0, socket.IPPROTO_TCP):
            if info[0] == socket.AF_INET6:
                continue
            hostname = info[4][0]
            logger.info('Creating messaging client on hostname={0}, port={1}'.format(hostname, MessagingConfig.PORT))
            self.client_list.append(Client(hostname, MessagingConfig.PORT, no_db_log=self._no_db_log))

        for client in self.client_list:
            client.connect()

    def stop(self):
        for client in self.client_list:
            client.disconnect()
