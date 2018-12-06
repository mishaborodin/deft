__author__ = 'Dmitry Golubkov'

import sys
import traceback
import logging


def get_exception_string():
    ex_info = sys.exc_info()
    ex_string = traceback.format_exception_only(*ex_info[:2])[-1]
    ex_string = ex_string[:-1].replace("[u'", "").replace("']", "")
    return ex_string


class Logger(object):

    @staticmethod
    def get():
        return logging.getLogger(__name__)
