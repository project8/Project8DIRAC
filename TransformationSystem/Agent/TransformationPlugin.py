"""
TransformationPlugin is a class wrapping the supported transformation plugins
"""
import os
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

        # Checking if snapshot.json is present in the lfn list for each run_id. Omit from dict if not present.
        for runID in runDict:
            lfns = runDict[runID]
            if not lfns:
                continue
            res = fc.getFileUserMetadata(lfns[0])
            if not res['OK']:
                sys.exit(-9)
            metadata = res['Value']
            inputDataQuery = {'DataType': 'data', 'DataLevel': 'RAW', 'run_id': metadata['Value']['run_id']}
            #[metadata.pop(key) for key in ['SoftwareVersion', 'DataLevel', 'DataExt', 'ConfigVersion', 'DataType', 'DataFlavor']]
            #metadata['DataLevel'] = 'RAW'
            #res = fc.findFilesByMetadata( metadata )
            #if not res['OK']:
            #    print('count not get files')
            #    sys.exit(-9)
            #filtered_data = {k:v for k,v in res['Value'] if 'snapshot.json' in k}
            #inputDataQuery = {'DataType': 'data', 'DataLevel': 'raw'}
	    #inputDataQuery.update({'run_id': '8603'})        
            #print(inputDataQuery)
            result = fc.findFilesByMetadata( inputDataQuery )
            if not result['OK']:
                print('count not get files')
                sys.exit(-9)
            files = []
            if result['OK']:
                files = result['Value']
                filtered_file = list({f for f in files if 'snapshot.json' in f})
                pprint(files)
                pprint(filtered_file)
            if not filtered_file:
                del runDict[runID]
            else:
        	#For each run_id, get list of event files from catalog to match with input lfn list.
                metadata['Value']['Datalevel'] = 'processed'
                metadata['Value']['DataFlavor'] = 'event'
        	result = fc.findFilesByMetadata( metadata['Value'] )
                if not result['OK']:
                    sys.exit(-9)
                    print('could not get files')
                if not set(result['Value']).issuperset(set(runDict[runID])):  #result['Value'] != runDict[runID]:
                    del runDict[runID]
        ops_dict = opsHelper.getOptionsDict('Transformations/')
        if not ops_dict['OK']:
            return ops_dict
        ops_dict = ops_dict['Value']
        PROD_DEST_DATA_SE = ops_dict.get('ProdDestDataSE', 'PNNL-PIC-SRM-SE')
        tasks = [(PROD_DEST_DATA_SE, runDict[runID]) for runID in runDict]
        return S_OK(tasks)
