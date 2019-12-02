__author__ = 'Dmitry Golubkov'

import os
import subprocess
from OpenSSL.crypto import load_certificate, FILETYPE_PEM
from deftcore.settings import VOMS_CERT_FILE_PATH, VOMS_KEY_FILE_PATH, X509_PROXY_PATH
from deftcore.log import Logger

logger = Logger.get()


class NoProxyException(Exception):
    def __init__(self):
        super(NoProxyException, self).__init__('Unable to initialize the valid VOMS proxy')


# noinspection PyBroadException, PyUnresolvedReferences
class VOMSClient(object):
    def __init__(self):
        self.lifetime = 43200
        self.voms = 'atlas:/atlas/Role=production'
        self.proxy_file_path = X509_PROXY_PATH

    def get(self, force=False):
        if (not self._is_proxy_valid()) or force:
            proxy_init_command = 'voms-proxy-init -valid {0}:00 -voms {1} -cert {2} -key {3} -out {4}'.format(
                self.lifetime / 3600,
                self.voms,
                VOMS_CERT_FILE_PATH,
                VOMS_KEY_FILE_PATH,
                self.proxy_file_path
            )
            try:
                process = subprocess.Popen(proxy_init_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                           shell=True)
                stdout = process.communicate()[0]
                logger.info('stdout={0}{1}'.format(os.linesep, stdout))
            except Exception as ex:
                raise Exception('voms-proxy-init process failed: {0}'.format(str(ex)))

            if not self._is_proxy_valid():
                raise NoProxyException()
        return self.proxy_file_path

    def remove(self):
        if self._is_proxy_valid():
            os.remove(self.proxy_file_path)

    def _is_proxy_valid(self):
        if not os.path.isfile(self.proxy_file_path):
            return False
        with open(self.proxy_file_path, 'rb') as proxy_file:
            try:
                cert_pem = proxy_file.read()
                proxy_file.close()
                x509 = load_certificate(FILETYPE_PEM, cert_pem)
                return not x509.has_expired()
            except Exception as ex:
                logger.warning('_is_proxy_valid failed: {0}'.format(ex))
                return False

    @property
    def valid(self):
        return self._is_proxy_valid()
