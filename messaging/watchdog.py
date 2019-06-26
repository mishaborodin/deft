__author__ = 'Dmitry Golubkov'

import threading
import time


class Watchdog(object):
    def __init__(self, logger, client_list, timeout=1):
        self.logger = logger
        self.client_list = client_list
        self.timeout = timeout
        self.thread = threading.Thread(target=self.worker, args=())
        self.thread.daemon = True

    def start(self):
        self.thread.start()

    def worker(self):
        while True:
            for client in self.client_list:
                if not client.is_connected():
                    self.logger.warning('Watchdog: client {0} is disconnected'.format(client.id))
                    client.connect()
            time.sleep(self.timeout)
