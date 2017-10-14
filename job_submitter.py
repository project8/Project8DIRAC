#! /usr/bin/env python

# Job submitter:
# Submit a job that will move files on the file catalog.
# It takes an input file as input containing a dictionary of the folders to move and their new location.
# It will split this file into sub-files
# For each subfolder to move, it will first download the files on the node, then add them to their new location on the FC and finally remove the files from the FC
# NB: this script does not protect the genealogy of the files (yet...)
# Author: M Guigue
# Creation: Oct 10 2017


from p8_base_script import BaseScript
from DIRAC.Core.Base import Script


import argparse
import pprint
import json
import os

# dirac requires that we set this
__RCSID__ = '$Id$'


class SubmitMoveJobs(BaseScript):

    '''
    Submit a job that will move files on the file catalog.
    It takes an input file as input containing a dictionary of the folders to move and their new location.
    For each subfolder to move, it will first download the files on the node, then add them to their new location on the FC and finally remove the files from the FC
    NB: this script does not protect the genealogy of the files (yet...)
    '''
    switches = [
        ('f:', 'filename=', 'Name of the file containing the location of the folders to move and where', 'details.yaml'),
        ('n:', 'number=', 'Number of folders to move per job', 50),
        ('l', 'local', 'Run the jobs locally', True)
    ]
    usage = [__doc__,
             'Usage:',
             '  %s [option|cfgfile] ' % Script.scriptName]

    def main(self):
        from DIRAC import gLogger, exit as DIRAC_exit
        from DIRAC.Interfaces.API.Job import Job
        from DIRAC.Interfaces.API.Dirac import Dirac

        gLogger.info("Opening {}".format(self.filename))
        import yaml
        with open(self.filename, 'r') as stream:
            try:
                theDict = yaml.load(stream)
            except yaml.YAMLError as exc:
                print(exc)
        gLogger.info("Found {} folder(s) to move".format(
            len(theDict.keys())))

        NJobs = int(len(theDict.keys()) / int(self.number)) + 1
        gLogger.info("Requires {} jobs".format(NJobs))

        # Dirac().submit(j, mode='local')

        iItem = 0
        # for iJob in range(0, NJobs):
        for iJob in range(0, 1):
            subDict = {}
            for i in range(0, int(self.number)):
                if iItem == int(len(theDict.keys())):
                    break
                aKey = theDict.keys()[iItem]
                aValue = theDict.values()[iItem]
                subDict.update({aKey: aValue})
                iItem = iItem + 1

            detailsfilename = 'details_{}.yaml'.format(iJob)
            gLogger.info(
                'Creating {}'.format(detailsfilename))
            with open(detailsfilename, 'w') as outfile:
                yaml.dump(subDict, outfile, default_flow_style=False)

            gLogger.info(
                'Creating job {}/{}'.format(iJob, NJobs))
            j = Job()
            j.setCPUTime(3000)
            j.setExecutable('move_data.sh',
                            arguments='{}'.format(detailsfilename))
            j.setName('move_files_FC_{}'.format(iJob))
            j.setDestination('DIRAC.PNNL.us')
            j.setLogLevel('debug')

            j.setInputSandbox(['move_data.py', 'move_data.sh',
                               '../utilities/p8dirac_safe_access.py', 'details_{}.yaml'.format(iJob)])
            j.setOutputSandbox(['std.err', 'std.out'])
            # submit the job
            dirac = Dirac()
            if self.local:
                result = dirac.submit(j, mode='local')
            else:
                result = dirac.submit(j)
            gLogger.info(
                'Job_submitter: Submission Result: {}'.format(j._toJDL()))
            gLogger.info('Job_submitter: Jod ID: {}'.format(result['Value']))

            gLogger.info("Cleaning up!")
            try:
                os.remove(detailsfilename)
            except:
                gLogger.info(
                    'Failed removing {}'.format(detailsfilename))
                sys.exit(1)
            gLogger.info(
                "{} removed!".format(detailsfilename))


# make it able to be run from a shell
if __name__ == "__main__":
    script = SubmitMoveJobs()
    script()


# # prepare the list of input data files
# inputdatalist = []
# for item in item_analysis['lfn_list']:
#     # print("Job_submitter: new item to upload {}".format(item))
#     inputdatalist.append('LFN:{}'.format(item))

# j.setInputData(inputdatalist)


# os.remove("details.json")
# print("details.json Removed!")
