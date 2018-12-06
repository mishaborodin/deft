__author__ = 'Dmitry Golubkov'

import os
import argparse
import time
from daemonize import Daemonize
import logging
import logging.handlers

pid = '../deftcore_daemon.pid'

formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] [%(module)s] [%(funcName)s:%(lineno)d] - %(message)s')
ch_formatter = logging.Formatter('%(message)s')

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(ch_formatter)

fh = logging.handlers.RotatingFileHandler('../logs/deftcore_daemon.log', maxBytes=16 * 1024 * 1024, backupCount=5)
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)

logger = logging.getLogger('deftcore_daemon')
logger.setLevel(logging.DEBUG)
logger.addHandler(ch)
logger.addHandler(fh)

keep_fds = [fh.stream.fileno()]


def main():
    from messaging.manager import Manager

    messaging_manager = Manager(no_db_log=args.nodb)
    messaging_manager.start()

    while True:
        time.sleep(5)


if __name__ == "__main__":
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'deftcore.settings')
    import django

    django.setup()

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-n',
        '--nodb',
        action='store_true',
        dest='nodb',
        default=False,
        help='disable writing logs into database'
    )
    parser.add_argument(
        '-f',
        '--foreground',
        action='store_true',
        dest='foreground',
        default=False,
        help='running daemon in foreground'
    )
    args = parser.parse_args()

    logger.info('Starting the daemon (args={0})'.format(args))

    daemon = Daemonize(
        app='deftcore daemon',
        pid=pid,
        action=main,
        keep_fds=keep_fds,
        verbose=True,
        logger=logger,
        foreground=args.foreground
    )

    daemon.start()

    logger.info('The daemon ended gracefully')
