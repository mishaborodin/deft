from django.core.management.base import BaseCommand
from taskengine.workflow_validation import run_test_interactive
from deftcore.log import Logger

logger = Logger.get()


class Command(BaseCommand):


    def handle(self, *args, **options):
        run_test_interactive()