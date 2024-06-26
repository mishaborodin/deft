__author__ = 'Dmitry Golubkov'

import os
import subprocess
from OpenSSL.crypto import load_certificate, FILETYPE_PEM
from deftcore.settings import VOMS_CERT_FILE_PATH, VOMS_KEY_FILE_PATH, X509_PROXY_PATH
from deftcore.log import Logger
from datetime import datetime, timedelta

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

    def get(self, force=False, log_std_streams=False):
        if (not self._is_proxy_valid()) or force:
            proxy_init_command = 'voms-proxy-init -valid {0}:00 -voms {1} -cert {2} -key {3} -out {4}'.format(
                int(self.lifetime / 3600),
                self.voms,
                VOMS_CERT_FILE_PATH,
                VOMS_KEY_FILE_PATH,
                self.proxy_file_path
            )
            try:
                process = subprocess.Popen(proxy_init_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                           shell=True)
                if log_std_streams:
                    stdout, stderr = process.communicate()
                    logger.info('stdout={0}{1}'.format(os.linesep, stdout))
                    logger.info('stderr={0}{1}'.format(os.linesep, stderr))
                else:
                    process.communicate()
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
                if x509.has_expired():
                    return False
                time_left = datetime.strptime(x509.get_notAfter().decode().rstrip('Z'), '%Y%m%d%H%M%S')
                time_diff = time_left - datetime.utcnow()
                return time_diff.total_seconds() > 3600
            except Exception as ex:
                logger.warning('_is_proxy_valid failed: {0}'.format(ex))
                return False

    @property
    def valid(self):
        return self._is_proxy_valid()
