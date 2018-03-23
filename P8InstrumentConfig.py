#!/usr/bin/env python
'''
A utility to extract a run metadata and get useful characteristics (trap configuration, ROI and central frequency...)
'''

from p8_base_script import BaseScript

from DIRAC import S_OK, S_ERROR, gLogger, exit as DIRACExit


# dirac requires that we set this
__RCSID__ = '$Id$'

class P8InstrumentConfig(object):
    '''
        Determine the instrument configuration (run metadata) and return a dictionary
    '''
    # switches = [
    #             ('', 'outputfile=', 'Save the instrument configuration into the given file', None),
    #            ]
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
                adict = metaDataDict.get('Value',{})
                # reformating of the output dictionary: the metadata are not lists...
                newdict = {}
                for key in sorted(adict.keys()):
                    value = adict[key]
                    if isinstance(value,list) and len(value)==1:
                        value = value[0]
                    newdict.update({key:value})
                return newdict
        print("Failed getting metadata from {} in {}:".format(runID,path))
        listErr = metaDataDict.get('CallStack')
        errMsg = ''
        for item in listErr:
            errMsg = errMsg+item
        print(errMsg)
        return None

    def _getParamFromCatalog(self,paramName):
        ''' 
        returns the value(s) of one metadata field
        '''
        path='/project8/dirac/proc'
        runID = self.args[0]        
        metaDataDict = getConfigFromCatalog(runID,path,3)
        return getValueFromDict(metaDataDict)

    def _getValueFromDict(self,dict,paramName):
        if str(paramName) in dict:
            value = dict.get(str(paramName))
            if isinstance(value,list) and len(value)==1:
                return value[0]
            else: 
                return value
        else:
            return None

    def saveConfigFromCatalog(self,runID,outputfile):
        adict = self.getConfigFromCatalog(runID)
        import json            
        with open(outputfile, 'w') as outfile:
            json.dump(adict, outfile)
        gLogger.error("Instrument config saved into {}".format(outputfile))
        return adict

    def getTrapConfig(self,runID):
        metaDataDict = self.getConfigFromCatalog(runID)
        if metaDataDict=={}:
            print('no metadata returned')
            return 
        listCoilsOn = []
        listCurrent = []
        for i in range(1,5):
            coilName = 'trap_coil_{}'.format(i)
            if int(self._getValueFromDict(metaDataDict,coilName+'_relay_status'))==1:
                listCoilsOn.append(i)
                listCurrent.append(self._getValueFromDict(metaDataDict,coilName+'_current_output'))
        # print(listCoilsOn)
        if len(listCoilsOn)==1:
            return ['harmonic_trap_{}'.format(listCoilsOn[0]),listCurrent]
        if len(listCoilsOn)==2:
            return ['bathtub_trap_{}{}'.format(listCoilsOn[0],listCoilsOn[1]),listCurrent]
        if len(listCoilsOn)>2:
            return ['unknown',[0]]

    def getCentralFrequency(self, runID):
        #TODO
        