#!/usr/bin/env python
'''
A utility to extract a run metadata, get useful characteristics (trap configuration, ROI and central frequency...)
and save it
'''

from p8_base_script import BaseScript

from P8InstrumentConfig import P8InstrumentConfig

from DIRAC import S_OK, S_ERROR, gLogger, exit as DIRACExit


# dirac requires that we set this
__RCSID__ = '$Id$'

class GetInstrumentConfig(BaseScript):
    '''
        Determine the instrument configuration (run metadata) and return a dictionary
    '''
    switches = [
                ('', 'outputfile=', 'Save the instrument configuration into the given file', None),
               ]
    
    def main(self):
        gLogger.info('args are: {}'.format(self.args))
        if len(self.args) > 1:
            gLogger.error('queries of multiple RID not yet supported')
            DIRAC_exit(1)
        configobject = P8InstrumentConfig()
        if isinstance(self.outputfile,str):
            adict = configobject.saveConfigFromCatalog(int(self.args[0]),self.outputfile)
        else: 
            gLogger.error("output file given is not a string")
        
# make it able to be run from a shell
if __name__ == "__main__":
    script = GetInstrumentConfig()
    script()
