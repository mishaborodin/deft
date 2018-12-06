__author__ = 'Dmitry Golubkov'

import os
import argparse
import time
from daemonize import Daemonize
from deftcore.log import Logger


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

    from deftcore.settings import BASE_DIR

    logger = Logger.get()
    pid = os.path.join(BASE_DIR, '../../deftcore_daemon.pid')
    keep_fds = []

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
