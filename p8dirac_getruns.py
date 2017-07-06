#!/usr/bin/env python
''' 
Get a list of runs that satisfy the metadata cuts give as inputs.

usage:
python p8dirac_getruns.py cut1 [cut2 cut3 ...]
or,
./p8dirac_checkmat.py cut1 [cut2 cut3 ...]

The format for the cuts is as specified for dirac-dms-find-lfns.  Enclose each
cut string in single quotes, e.g.:

./getruns.py 'DAQ=RSA5106B' 'run_id>2807'

will return all RSA runs with run_id greater than 2807 to stdout.  Enclose the 
above to use the outpus as arguments to some other command, e.g.:

./job_submitter $(./getruns.py 'DAQ=RSA5106B' 'run_id>2807')
'''


import subprocess
import argparse

from DIRAC.Core.Base import Script
Script.parseCommandLine()

from DIRAC.Interfaces.API.Dirac import Dirac
dirac = Dirac()

from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
fcc = FileCatalogClient()

def p8dirac_getruns(cut_strings):

    basepath = '/project8/dirac/data'

    command = 'dirac-dms-find-lfns Path={} '.format(basepath)
    for cut_string in cut_strings:
        command = '{} \'{}\''.format(command, cut_string)

    lfn_list = sorted([lfn for lfn in subprocess.check_output(command, shell=True).split('\n')[:-1]
                        if lfn.endswith('.mat') or lfn.endswith('.MAT')])

    # this line for runs >= 2773 
    run_list = sorted(int(lfn.split('/')[6]) for lfn in lfn_list)
    # this line for runs < 2773 
    #run_list = sorted(int(lfn.split('/')[4]) for lfn in lfn_list)

    run_set = sorted(frozenset(run_list))

    for run in run_set:
        print(' {}'.format(run))

    
if __name__ == "__main__":

    parser = argparse.ArgumentParser(description ='"Get a list of run_ids that satisfy some metadata cuts, each enclosed in single quotes.')
    parser.add_argument('cut_strings', help = 'A string specifying a metadata cut', nargs = '+')
    args = parser.parse_args()

    p8dirac_getruns(args.cut_strings)

