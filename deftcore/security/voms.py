__author__ = 'Dmitry Golubkov'

import os
import subprocess
import datetime
import gridproxy
import gridproxy.voms
from deftcore.settings import VOMS_CERT_FILE_PATH, VOMS_KEY_FILE_PATH, X509_PROXY_PATH


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
                process.communicate()
            except Exception as ex:
                raise Exception('voms-proxy-init process failed: {0}'.format(str(ex)))
        return self.proxy_file_path

    def remove(self):
        if self._is_proxy_valid():
            os.remove(self.proxy_file_path)

    def _is_proxy_valid(self):
        if not os.path.isfile(self.proxy_file_path):
            return False
        voms_client = gridproxy.voms.VOMS()
        with open(self.proxy_file_path, 'r') as proxy_file:
            try:
                _, chain = gridproxy.load_proxy(proxy_file.read())
                voms_client.from_x509_stack(chain)
            except Exception:
                return False
        not_after = voms_client.not_after.replace(tzinfo=None)
        return not_after >= datetime.datetime.now()
