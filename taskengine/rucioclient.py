__author__ = 'Dmitry Golubkov'

# howTo install Rucio client
# source env/bin/activate
# pip install rucio-clients-atlas

import os
import re
from deftcore.log import Logger
from deftcore.settings import RUCIO_ACCOUNT_NAME
from rucio.client import Client
from rucio.common.exception import CannotAuthenticate, DataIdentifierAlreadyExists, DataIdentifierNotFound, Duplicate, \
    RucioException, InputValidationError
from deftcore.security.voms import VOMSClient

logger = Logger.get()


class RucioClient(object):
    def __init__(self):
        try:
            # set up Rucio environment
            os.environ['RUCIO_ACCOUNT'] = RUCIO_ACCOUNT_NAME
            os.environ['RUCIO_AUTH_TYPE'] = 'x509_proxy'
            os.environ['X509_USER_PROXY'] = self._get_proxy()
            self.client = Client()
        except CannotAuthenticate as ex:
            logger.critical('RucioClient: authentication failed: {0}'.format(str(ex)))
        except Exception as ex:
            logger.critical('RucioClient: initialization failed: {0}'.format(str(ex)))

    def _get_proxy(self):
        return VOMSClient().get()

    def verify(self):
        try:
            rucio_server_info = self.client.ping()
            if rucio_server_info:
                logger.info('RucioClient: Rucio server version is {0}'.format(rucio_server_info['version']))
                return True
            else:
                return False
        except Exception as ex:
            logger.exception('RucioClient: exception occurred during verifying: {0}'.format(str(ex)))
            return False

    def extract_scope(self, dsn):
        if dsn.find(':') > -1:
            scope, name = dsn.split(':')[0], dsn.split(':')[1]
            if name.endswith('/'):
                name = name[:-1]
            return scope, name
        else:
            scope = dsn.split('.')[0]
            if dsn.startswith('user') or dsn.startswith('group'):
                scope = '.'.join(dsn.split('.')[0:2])
            if dsn.endswith('/'):
                dsn = dsn[:-1]
            return scope, dsn

    def list_datasets(self, pattern):
        result = list()
        match = re.match(r'^\*', pattern)
        if not match:
            scope, dataset = self.extract_scope(pattern)
            collection = 'dataset'
            if pattern.endswith('/'):
                collection = 'container'
            filters = {'name': dataset}
            # FIXME: use type='collection'
            for name in self.client.list_dids(scope, filters, type=collection):
                result.append('{0}:{1}'.format(scope, name))
        return result

    def list_datasets_in_container(self, container):
        dataset_names = list()

        if container.endswith('/'):
            container = container[:-1]

        scope, container_name = self.extract_scope(container)

        try:
            if self.client.get_metadata(scope, container_name)['did_type'] == 'CONTAINER':
                for e in self.client.list_content(scope, container_name):
                    dsn = '{0}:{1}'.format(e['scope'], e['name'])
                    if e['type'] == 'DATASET':
                        dataset_names.append(dsn)
                    elif e['type'] == 'CONTAINER':
                        names = self.list_datasets_in_container(dsn)
                        # FIXME: check not exist
                        dataset_names.extend(names)
        except DataIdentifierNotFound:
            # FIXME
            pass
        return dataset_names

    def list_files_in_dataset(self, dsn):
        filename_list = list()
        scope, dataset = self.extract_scope(dsn)
        files = self.client.list_files(scope, dataset, long=False)
        for file_name in [e['name'] for e in files]:
            filename_list.append(file_name)
        return filename_list

    def get_number_files(self, dsn):
        number_files = 0
        if self.is_dsn_container(dsn):
            for name in self.list_datasets_in_container(dsn):
                number_files += self.get_number_files_from_metadata(name)
        else:
            number_files += self.get_number_files_from_metadata(dsn)
        return number_files

    def get_number_events(self, dsn):
        scope, dataset = self.extract_scope(dsn)
        metadata = self.client.get_metadata(scope=scope, name=dataset)
        return int(metadata['events'] or 0)

    def get_number_files_from_metadata(self, dsn):
        scope, dataset = self.extract_scope(dsn)
        try:
            metadata = self.client.get_metadata(scope=scope, name=dataset)
            return int(metadata['length'] or 0)
        except Exception as ex:
            raise Exception('DDM Error: rucio_client.get_metadata failed ({0}) ({1})'.format(str(ex), dataset))

    def erase(self, dsn, undo=False):
        scope, name = self.extract_scope(dsn)
        lifetime = 86400
        if undo:
            lifetime = None
        self.client.set_metadata(scope=scope, name=name, key='lifetime', value=lifetime)

    def register_dataset(self, dsn, files=None, statuses=None, meta=None, lifetime=None):
        """
        :param dsn: the DID name
        :param files: list of file names
        :param statuses: dictionary with statuses, like {'monotonic':True}.
        :param meta: meta-data associated with the data identifier is represented using key/value pairs in a dictionary.
        :param lifetime: DID's lifetime (in seconds).
        """
        scope, name = self.extract_scope(dsn)
        dids = None
        if files:
            dids = list()
            for file_ in files:
                file_scope, file_name = self.extract_scope(file_)
                dids.append({'scope': file_scope, 'name': file_name})
        self.client.add_dataset(scope, name, statuses=statuses, meta=meta, lifetime=lifetime, files=dids)

    def register_files_in_dataset(self, dsn, files):
        scope, name = self.extract_scope(dsn)
        dids = list()
        for file_ in files:
            file_scope, file_name = self.extract_scope(file_)
            dids.append({'scope': file_scope, 'name': file_name})
        self.client.attach_dids(scope, name, dids)

    def register_container(self, dsn, datasets=None):
        if dsn.endswith('/'):
            dsn = dsn[:-1]
        scope, name = self.extract_scope(dsn)
        self.client.add_container(scope=scope, name=name)
        if datasets:
            dsns = list()
            for dataset in datasets:
                dataset_scope, dataset_name = self.extract_scope(dataset)
                dsns.append({'scope': dataset_scope, 'name': dataset_name})
            self.client.add_datasets_to_container(scope=scope, name=name, dsns=dsns)

    def register_datasets_in_container(self, dsn, datasets):
        if dsn.endswith('/'):
            dsn = dsn[:-1]
        scope, name = self.extract_scope(dsn)
        dsns = list()
        for dataset in datasets:
            dataset_scope, dataset_name = self.extract_scope(dataset)
            dsns.append({'scope': dataset_scope, 'name': dataset_name})
        self.client.add_datasets_to_container(scope=scope, name=name, dsns=dsns)

    def delete_datasets_from_container(self, dsn, datasets):
        if dsn.endswith('/'):
            dsn = dsn[:-1]
        scope, name = self.extract_scope(dsn)
        dsns = list()
        for dataset in datasets:
            dataset_scope, dataset_name = self.extract_scope(dataset)
            dsns.append({'scope': dataset_scope, 'name': dataset_name})
        self.client.detach_dids(scope=scope, name=name, dids=dsns)

    def get_metadata_attribute(self, dsn, attribute_name):
        scope, dataset = self.extract_scope(dsn)
        metadata = self.client.get_metadata(scope=scope, name=dataset)
        if attribute_name in metadata.keys():
            return metadata[attribute_name]
        else:
            return None

    def is_dsn_container(self, dsn):
        scope, dataset = self.extract_scope(dsn)
        metadata = self.client.get_metadata(scope=scope, name=dataset)
        return bool(metadata['did_type'] == 'CONTAINER')

    def is_dsn_dataset(self, dsn):
        scope, dataset = self.extract_scope(dsn)
        metadata = self.client.get_metadata(scope=scope, name=dataset)
        return bool(metadata['did_type'] == 'DATASET')

    def is_dsn_exist(self, dsn):
        scope, dataset = self.extract_scope(dsn)
        try:
            return bool(self.client.get_metadata(scope=scope, name=dataset))
        except DataIdentifierNotFound:
            return False

    def get_campaign(self, dsn):
        scope, dataset = self.extract_scope(dsn)
        metadata = self.client.get_metadata(scope=scope, name=dataset)
        return str(metadata['campaign'])

    def get_nevents_per_file(self, dsn):
        number_files = self.get_number_files(dsn)
        if not number_files:
            raise ValueError('Dataset {0} has no files'.format(dsn))
        number_events = self.get_number_events(dsn)
        if not number_files:
            raise ValueError('Dataset {0} has no events or corresponding metadata (nEvents)'.format(dsn))
        round_up = lambda num: int(num + 1) if int(num) != num else int(num)
        return round_up(float(number_events) / float(number_files))

    def get_datasets_and_containers(self, input_data_name, datasets_contained_only=False):
        data_dict = {'containers': list(), 'datasets': list()}

        if input_data_name[-1] == '/':
            input_container_name = input_data_name
            input_data_name = input_data_name[:-1]
        else:
            input_container_name = '{0}/'.format(input_data_name)

        # searching containers first
        for name in self.list_datasets(input_container_name):
            if self.is_dsn_container(name):
                if name[-1] == '/':
                    data_dict['containers'].append(name)
                else:
                    data_dict['containers'].append('{0}/'.format(name))

        # searching datasets
        if datasets_contained_only and len(data_dict['containers']):
            for container_name in data_dict['containers']:
                dataset_names = self.list_datasets_in_container(container_name)
                data_dict['datasets'].extend(dataset_names)
        else:
            enable_pattern_search = True
            names = self.list_datasets(input_data_name)
            if len(names) > 0:
                if names[0].split(':')[-1] == input_data_name.split(':')[-1] and self.is_dsn_dataset(names[0]):
                    data_dict['datasets'].append(names[0])
                    enable_pattern_search = False
            if enable_pattern_search:
                for name in self.list_datasets("{0}*".format(input_data_name)):
                    # FIXME
                    is_sub_dataset = \
                        re.match(r"%s.*_(sub|dis)\d*" % input_data_name.split(':')[-1], name.split(':')[-1],
                                 re.IGNORECASE)
                    is_o10_dataset = \
                        re.match(r"%s.*.o10$" % input_data_name.split(':')[-1], name.split(':')[-1], re.IGNORECASE)
                    if not self.is_dsn_container(name) and not is_sub_dataset and not is_o10_dataset:
                        data_dict['datasets'].append(name)

        return data_dict
