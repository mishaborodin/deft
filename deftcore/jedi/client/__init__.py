__author__ = 'Dmitry Golubkov'

import os
import imp
import sys
from deftcore.settings import JEDI_CLIENT_PATH
from deftcore.security.voms import VOMSClient
from deftcore.log import Logger

logger = Logger.get()


def _x509():
    client = VOMSClient()
    x509 = client.get()
    if os.access(x509, os.R_OK):
        logger.debug("x509 = %s" % x509)
        return x509
    logger.warning('No valid grid proxy certificate found')
    return ''


with open(JEDI_CLIENT_PATH, 'r') as fp:
    client_data = fp.read()
jedi_client_module = imp.new_module('Client')
exec(client_data, jedi_client_module.__dict__)
jedi_client_module.__dict__['_x509'] = _x509

sys.modules[jedi_client_module.__name__] = jedi_client_module
exec('from {0} import *'.format(jedi_client_module.__name__))
