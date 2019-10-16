__author__ = 'Dmitry Golubkov'

import requests
import urlparse
from deftcore.settings import AGIS_API_BASE_URI
from deftcore.log import Logger

logger = Logger.get()


class AGISClient(object):
    def __init__(self):
        self.base_uri = AGIS_API_BASE_URI

    def _list_panda_resources(self):
        url = urlparse.urljoin(self.base_uri, 'request/pandaresource/query/list/?json')
        r = requests.get(url)
        return r.json()

    def _list_swreleases(self):
        url = urlparse.urljoin(self.base_uri, 'jsoncache/list_swreleases.json')
        r = requests.get(url)
        return r.json()

    def _list_blacklisted_rses(self):
        url = urlparse.urljoin(self.base_uri, 'request/ddmendpointstatus/query/list/?json')
        r = requests.get(url)
        return r.json()

    def get_blacklisted_rses(self):
        rses = self._list_blacklisted_rses()
        return [rse for rse in rses.keys()]

    def get_sites(self):
        panda_resources = self._list_panda_resources()
        return [r['name'] for r in panda_resources]

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
