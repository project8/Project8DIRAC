#!/usr/bin/env python
'''
A utility to display the ancestors of an LFN
'''

from p8_base_script import BaseScript

from DIRAC import S_OK, S_ERROR, gLogger, exit as DIRACExit


# dirac requires that we set this
__RCSID__ = '$Id$'

class InstrumentConfig(BaseScript):
    '''
        Determine the instrument configuration (run metadata) and return a dictionary
    '''
    switches = [
                ('', 'outputfile=', 'Save the instrument configuration into the given file', None),
               ]
    def getConfigFromCatalog(self, runID):
        '''
        returns a dictionary containing the metadata values of a run
        '''
        path='/project8/dirac/proc'
        nTries=3

        from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient    
        fcc = FileCatalogClient()
        metaDataDict = fcc.getCompatibleMetadata({'run_id':{'=':int(runID)}}, path=path)
        for i in range(nTries):
            if metaDataDict.get('OK',False):
                return metaDataDict.get('Value',{})
        print("Failed getting metadata from {} in {}:".format(runID,path))
        listErr = metaDataDict.get('CallStack')
        errMsg = ''
        for item in listErr:
            errMsg = errMsg+item
        print(errMsg)
        return None

    def getParamFromCatalog(self,paramName):
        ''' 
        returns the value(s) of one metadata field
        '''
        path='/project8/dirac/proc'
        runID = self.args[0]        
        metaDataDict = getConfigFromCatalog(runID,path,3)
        return getValueFromDict(metaDataDict)

    def getValueFromDict(self,dict,paramName):
        if str(paramName) in dict:
            value = dict.get(str(paramName))
            if isinstance(value,list) and len(value)==1:
                return value[0]
            else: 
                return value
        else:
            return None

    def saveConfigFromCatalog(self,runID,outputfile):
        adict = self.getConfigFromCatalog(int(self.args[0]))
        newdict = {}
        for key, value in adict.items():
            if isinstance(value,list) and len(value)==1:
                value = value[0]
            newdict.update({key:value})
        import json            
        with open(str(self.outputfile), 'w') as outfile:
            json.dump(newdict, outfile)

    def main(self):

        # from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
        # fcc = FileCatalogClient()
        #signature: getFileAncestors( self, lfns, depths, timeout = 120 ):
        gLogger.info('args are: {}'.format(self.args))
        if len(self.args) > 1:
            gLogger.error('queries of multiple RID not yet supported')
            DIRAC_exit(1)
        adict = self.getConfigFromCatalog(int(self.args[0]))
        newdict = {}
        for key, value in adict.items():
            if isinstance(value,list) and len(value)==1:
                value = value[0]
            newdict.update({key:value})
            gLogger.error("{}:\t {}".format(key,value))
        if isinstance(self.outputfile,str):
            gLogger.info("Saving instrument config into {}".format(self.outputfile))
            import json            
            with open(str(self.outputfile), 'w') as outfile:
                json.dump(newdict, outfile)
        # result = fcc.findDirectoriesByMetadata({'run_id':{'=':int(self.args[0])}}, path='/project8/dirac/proc')
        # gLogger.info("\nparsing to: {}\n".format([path.split(self.args[0].zfill(9),1)[-1] for _,path in result['Value'].items()]))
        # output_dirs = [path.split(self.args[0].zfill(9),1)[-1] for _,path in result['Value'].items()]
        # output_dirs.remove('')
        # output_dirs.sort()
        # gLogger.always("found analysis outputs:")
        # for path in output_dirs:
        #     gLogger.always('  > {}'.format(path))

# class TrapConfig(InstrumentConfig):
    


#     def main():
        
#     # def getTrapConfig(runID):

#         metaDataDict = getConfigFromCatalog(runID)
#         if metaDataDict=={}:
#             print('no metadata returned')
#             return 
#         listCoilsOn = []
#         listCurrent = []
#         for i in range(1,5):
#             coilName = 'trap_coil_{}'.format(i)
#             if int(getValueFromDict(metaDataDict,coilName+'_relay_status'))==1:
#                 listCoilsOn.append(i)
#                 listCurrent.append(getValueFromDict(metaDataDict,coilName+'_current_output'))
#         # print(listCoilsOn)
#         if len(listCoilsOn)==1:
#             return ['harmonic_trap_{}'.format(listCoilsOn[0]),listCurrent]
#         if len(listCoilsOn)==2:
#             return ['bathtub_trap_{}{}'.format(listCoilsOn[0],listCoilsOn[1]),listCurrent]
#         if len(listCoilsOn)>2:
#             return ['unknown',[0]]

# make it able to be run from a shell
if __name__ == "__main__":
    script = InstrumentConfig()
    script()
