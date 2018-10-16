"""
TransformationPlugin is a class wrapping the supported transformation plugins
"""

from collections import Counter

from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.TransformationSystem.Agent.TransformationPlugin \
        import TransformationPlugin as DIRACTransformationPlugin
from DIRAC.TransformationSystem.Client.TransformationClient \
        import TransformationClient
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations

from Project8DIRAC.TransformationSystem.Client.Utilities import PluginUtilities

__RCSID__ = "$Id$"

class TransformationPlugin(DIRACTransformationPlugin):

    """ Project8 extension of TransformationPlugin
    """

    def __init__(self, plugin, transClient=None, dataManager=None):
        super(TransformationPlugin, self).__init__(
                plugin, transClient=transClient, dataManager=dataManager)
        self.opsHelper = Operations()
        self.util = PluginUtilities(plugin, transClient, dataManager)

    def _P8FilterForRAWData(self):
        """
        Generate a task for Project8 raw data
        """
        filtered_data = {k:v for k,v in self.data.items() if '.egg' in k}
        return self.util.groupBySize(filtered_data, self.params['Status'])

    def _P8Merge(self):
        """
        Generate a task that merges processed data
        """
        files = dict(self.data).keys()

        runDict = {}
        for f in files:
            res = self.util.getRunID(f)
            if not res['OK']:
                continue # Should we continue, or return S_ERROR
            runID = res['Value']
            runDict.setdefault(runID, []).append(f)

        for runIDs in runDict:
            lfn = runDict[runIDs][0]
            #lfn = '/project8/dirac/ts_processed/000yyyxxx/000007xxx/000007000/katydid_v2.13.0/termite_v1.1.1/rid000007000_10_event.root'
            metadata = fc.getFileUserMetadata(lfn)
            run_id = metadata['Value']['run_id']
            software_tag = metadata['Value']['SoftwareVersion']
            config_tag = metadata['Value']['ConfigVersion']
            print(software_tag)
            basename = os.path.basename(lfn)
            stringtoremove = [software_tag + '/' + config_tag + '/' + basename]
            lfn_raw_data = lfn.replace(stringtoremove[0],'')
            lfn_raw_data = lfn_raw_data.replace('ts_processed','data')
            print([lfn_raw_data + 'rid00000' + str(run_id) + '_snapshot.json'])
            res = fc.isFile([lfn_raw_data + 'rid00000' + str(run_id) + '_snapshot.json'])
            if not res['Value']['Successful'].values()[0]:
                del runDict[runIDs]

        ops_dict = opsHelper.getOptionsDict('Transformations/')
        if not ops_dict['OK']:
            return ops_dict
        ops_dict = ops_dict['Value']
        PROD_DEST_DATA_SE = ops_dict.get('ProdDestDataSE', 'PNNL-PIC-SRM-SE')
        tasks = [(PROD_DEST_DATA_SE, runDict[runID]) for runID in runDict]
        return S_OK(tasks)
