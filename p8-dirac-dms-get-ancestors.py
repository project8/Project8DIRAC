#!/usr/bin/env python
'''
A utility to display the ancestors of an LFN
'''

from p8_base_script import BaseScript

# dirac requires that we set this
__RCSID__ = '$Id$'


class GetAncestors(BaseScript):
    '''
        Determine the analysis that have been done for a given RID (based on directories that exist)

        Positional arg is a full LFN
    '''
    switches = [
        ('u:', 'depth=', 'Max number of generations to trace', 2),
    ]

    def main(self):
        from DIRAC import gLogger, exit as DIRAC_exit
        from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
        fcc = FileCatalogClient()
        # signature: getFileAncestors( self, lfns, depths, timeout = 120 ):
        result = fcc.getFileAncestors(self.args, int(self.depth))
        worked = result['Value']['Successful']
        for lfn, ancestors in worked.items():
            #gLogger.info("lfn,a -> \n{},{}".format(lfn,ancestors))
            gLogger.always('{}:\n{}'.format(lfn, '\n'.join(
                ['    > {}'.format(a) for a in ancestors] or ['    > None'])))
        #gLogger.info('result is:\n{}'.format(result['Value']['Successful']))


# make it able to be run from a shell
if __name__ == "__main__":
    script = GetAncestors()
    script()
