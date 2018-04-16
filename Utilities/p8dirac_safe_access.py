#!/usr/bin/env python
''' 
A production-hard file catalog access method for grid jobs.

usage:
p8dirac_safe_access(mode, lfn, pfn, se, retries, wait):
'''

import argparse
import time

from DIRAC.Core.Base import Script
Script.parseCommandLine()

from DIRAC.Interfaces.API.Dirac import Dirac
dirac = Dirac()
    
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
fcc = FileCatalogClient()


# global defaults
default_mode = 'get'
default_SE = 'PNNL-HEP-SRM-SE'
default_wait = 30
default_retries = 5

def p8dirac_safe_access(mode, lfn, pfn, se, retries, wait):

    status = {}
    
    for i in range(retries):
    
        if mode == 'get':
            status = dirac.getFile(lfn)
        elif mode == 'add':
            status = dirac.addFile(lfn, pfn, se)
        elif mode == 'remove':
            status = dirac.removeFile(lfn)
        elif mode == 'replace':
            if len(dirac.getReplicas(lfn)['Value']['Successful'].keys()) > 0:
                status = dirac.removeFile(lfn)
            status = dirac.addFile(lfn, pfn, se)
        else:
            print('p8dirac_safe_access: invalid access mode {}'.format(mode))
            return status

        if status['OK']:
            break
        else:
            print('p8dirac_safe_access: failed to {} {} with message: {}.'.format(mode, lfn, status['Message']))
            print('Will try {} more times before giving up.'.format(retries - (i+1)))
            time.sleep(wait)


    if not status['OK']:
        print('p8dirac_safe_access: failed to {} {} in {} attempts'.format(mode, lfn, retries))

    return status




if __name__ == "__main__":

    # parse input arguments
    parser = argparse.ArgumentParser(description = 'A production-hard catalog access method.')

    ##?? BAV - how to make some of these optional, consistent with the Script.parseCommandLine() call above?
    parser.add_argument('mode',    help = 'a catalog access mode', choices = ['get', 'add', 'remove', 'replace'], type = str, default = default_mode)
    parser.add_argument('lfn',     help = 'the catalog LFN',                                                      type = str)
    parser.add_argument('pfn',     help = 'the local PFN',                                                        type = str, default = None)
    parser.add_argument('se',      help = 'the storage element to receive the physical file (add mode only)',     type = str, default = default_SE)
    parser.add_argument('retries', help = 'the number of times to attempt access',                                type = int, default = default_retries)
    parser.add_argument('wait',    help = 'the number of seconds between attempts',                               type = int, default = default_wait)
    
    args = parser.parse_args()
    
    status = p8dirac_safe_access(args.mode, args.lfn, args.pfn, args.se, args.retries, args.wait)

    print(status)

