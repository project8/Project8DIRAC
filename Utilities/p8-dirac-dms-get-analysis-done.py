#!/usr/bin/env python
'''
A utility to display the ancestors of an LFN
'''

from p8_base_script import BaseScript

# dirac requires that we set this
__RCSID__ = '$Id$'

class GetAnalysisDone(BaseScript):
    '''
        Determine the analysis that have been done for a given RID (based on directories that exist)
    '''
    switches = [
                ('', 'rid=', 'RunID to look for results', None),
               ]
    def main(self):
        from DIRAC import gLogger, exit as DIRAC_exit
        from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
        fcc = FileCatalogClient()
        #signature: getFileAncestors( self, lfns, depths, timeout = 120 ):
        gLogger.info('args are: {}'.format(self.args))
        if len(self.args) > 1:
            gLogger.error('queries of multiple RID not yet supported')
            DIRAC_exit(1)
        result = fcc.findDirectoriesByMetadata({'run_id':{'=':int(self.args[0])}}, path='/project8/dirac/proc')
        gLogger.info("\nparsing to: {}\n".format([path.split(self.args[0].zfill(9),1)[-1] for _,path in result['Value'].items()]))
        output_dirs = [path.split(self.args[0].zfill(9),1)[-1] for _,path in result['Value'].items()]
        output_dirs.remove('')
        output_dirs.sort()
        gLogger.always("found analysis outputs:")
        for path in output_dirs:
            gLogger.always('  > {}'.format(path))


# make it able to be run from a shell
if __name__ == "__main__":
    script = GetAnalysisDone()
    script()
