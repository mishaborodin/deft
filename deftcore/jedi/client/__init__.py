__author__ = 'Dmitry Golubkov'

import os
import types
import sys
from deftcore.settings import JEDI_CORE_UTILS_PATH, JEDI_CLIENT_PATH
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


def import_module(path, name):
    with open(path, 'r') as fp:
        module_data = fp.read()
    module = types.ModuleType(name)
    exec(module_data, module.__dict__)
    sys.modules[module.__name__] = module
    return module


import_module(JEDI_CORE_UTILS_PATH, 'pandaserver.srvcore.CoreUtils')
jedi_client_module = import_module(JEDI_CLIENT_PATH, 'Client')
jedi_client_module.__dict__['_x509'] = _x509
exec('from {0} import *'.format(jedi_client_module.__name__))
