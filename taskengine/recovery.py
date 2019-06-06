__author__ = 'Dmitry Golubkov'

from taskengine.rucioclient import RucioClient
from taskengine.models import JEDIDatasetContent
from deftcore.log import Logger

logger = Logger.get()


def recovery_lost_files(dsn, files=None, dry_run=True):
    if not dsn:
        return

    if not files:
        rucio_client = RucioClient()
        scope, dataset = rucio_client.extract_scope(dsn)
        files_in_rucio = rucio_client.list_files_in_dataset(dsn)
        types = ','.join(["'output'", "'log'"])
        status = 'finished'
        query = \
            "SELECT c.lfn, c.datasetid FROM ATLAS_PANDA.JEDI_DATASETS d, ATLAS_PANDA.JEDI_DATASET_CONTENTS c " + \
            "WHERE d.jediTaskID=c.jediTaskID AND d.datasetID=c.datasetID AND d.type IN ({0}) ".format(types) + \
            "AND c.status='{0}' AND d.datasetName='{1}'".format(status, dataset)
        files = [c.filename for c in JEDIDatasetContent.objects.raw(query) if c.filename not in files_in_rucio]

    logger.info('recovery_lost_files, found {0} lost files: {1}'.format(len(files), ', '.join(files)))
