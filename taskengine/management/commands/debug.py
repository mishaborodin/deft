__author__ = 'Dmitry Golubkov'

from django.core.management.base import BaseCommand
from taskengine.taskdef import TaskDefinition
from deftcore.log import Logger

logger = Logger.get()


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            '-t',
            '--types',
            action='store',
            dest='request_types',
            type=lambda option, opt, value, p: setattr(p.values, option.dest, value.split(',')),
            default=None,
            help=''
        )

    def handle(self, *args, **options):
        request_types = None
        if 'request_types' in options.keys():
            request_types = options['request_types']
        engine = TaskDefinition()
        engine.process_requests(restart=False, no_wait=True, debug_only=True, request_types=request_types)
