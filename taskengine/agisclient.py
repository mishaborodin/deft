__author__ = 'Dmitry Golubkov'

import requests
import urllib.parse
from deftcore.settings import AGIS_API_BASE_URL, X509_PROXY_PATH,  VOMS_CERT_FILE_PATH, VOMS_KEY_FILE_PATH
from deftcore.log import Logger
import json

logger = Logger.get()


class AGISClient(object):
    def __init__(self, cert=(VOMS_CERT_FILE_PATH, VOMS_KEY_FILE_PATH)):
        self.base_url = AGIS_API_BASE_URL
        self.cert = cert

    def _get_url(self, command, postfix=''):
        if 'cache' in command:
            return urllib.parse.urljoin(self.base_url,command)
        else:
            return '{0}/{1}/query/?json{2}'.format(self.base_url, command, postfix)

    def _get_command(self, command, postfix=''):
        url = self._get_url(command, postfix)
        response = requests.get(url, cert=self.cert, verify='/etc/ssl/certs/CERN-bundle.pem')
        if response.status_code != requests.codes.ok:
            response.raise_for_status()
        content = json.loads(response.content)
        return content

    def _list_panda_resources(self):
        return self._get_command('atlas/pandaqueue')

    def _list_panda_queues_sw_tags(self):
        return self._get_command('atlas/pandaqueue','&preset=tags')

    def _list_swreleases(self):
        try:
            result = self._get_command('core/swrelease')
            if (type(result) is dict) and ('error' in result):
                raise RuntimeError(result['error'])
            return result
        except (requests.exceptions.RequestException,RuntimeError) as ex:
            logger.warning('_list_swreleases failed. Using failover url to list SW releases: {0}'.format(ex))
            return self._get_command('cache/swreleases.json')



    def _list_blacklisted_rses(self):
        return self._get_command('atlas/ddmendpointstatus')

    def list_site_sw_containers(self, site_name):
        sites_tags = self._list_panda_queues_sw_tags()
        if (site_name in sites_tags) and ('nightlies' not in sites_tags[site_name]['cvmfs']):
            return [x['container_name'] for x in sites_tags[site_name]['tags'] if x['container_name']]
        return []

    def get_blacklisted_rses(self):
        rses = self._list_blacklisted_rses()
        return list(rses.keys())

    def get_sites(self):
        panda_resources = self._list_panda_resources()
        return list(panda_resources.keys())

    def get_cmtconfig(self, cache):
        """
        :param cache: string in format 'CacheName-CacheRelease', for example, 'AtlasProduction-20.20.7.1'
        :return: list of available values of cmtconfig
        """
        release = cache.split('-')[-1]
        project = cache.split('-')[0]
        cmtconfig_list = list()
        swreleases = self._list_swreleases()
        for swrelease in swreleases:
            if swrelease['release'] == release and swrelease['project'] == project:
                cmtconfig_list.append(swrelease['cmtconfig'])
        return cmtconfig_list
