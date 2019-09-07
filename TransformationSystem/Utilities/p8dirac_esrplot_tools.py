#!/usr/bin/env python

from DIRAC.Core.Base import Script
Script.parseCommandLine()

from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC import gLogger, gConfig, S_OK, S_ERROR

import os
import sys
import json
import collections
import subprocess
import datetime
import upload_esr

ops_dict = Operations().getOptionsDict('Transformations/')
if not ops_dict['OK']:
    print('Failed to get SE information from DIRAC Operation config: %s'
            % ops_dict['Message'])
    sys.exit(-9)

ops_dict = ops_dict['Value']
PROD_DEST_DATA_SE = ops_dict.get('ProdDestDataSE', 'PNNL-PIC-SRM-SE')
PROD_DEST_MONITORING_SE = ops_dict.get('ProdDestMonitoringSE', '')


###################
# Get the Job LFN #
################### 
def getPlotJobLFNs():
    # Get the LFNs associated with the job on the machine
    jobID = int(os.environ['JOBID'])

    # Get the input data
    dirac = Dirac()
    res = dirac.getJobInputData(jobID)
    if not res['OK']:
        print('Failed to get job input data: %s' % res['Message'])

    # Try to extract the LFNs
    lfns = []
    try:
        lfns = res['Value'][jobID]
    except ValueError:
        print('Failed to extract LFN information')

    # Clean up LFN
    input_lfns = [lfn.replace('LFN:', '') for lfn in lfns]
    if len(input_lfns) == 0:
        print('No JSON files found')
    
    if len(input_lfns) == 1:
        input_lfns = input_lfns[0]
    else:
        print("Length of lfns is not 1")
    #print('Input lfn is: %s' %input_lfns)
    print('entering this plot_config.\n')
    # Write out lfn etc. to a config file
    exDict = {'input_lfns': input_lfns, 'input_file': os.path.basename(input_lfns)}
    with open('plot_config.txt', 'w') as file:
        file.write(json.dumps(exDict))
    return input_lfns

########################################
# Upload and register files with DIRAC #
########################################
def uploadJobOutputROOT():   
    jsonfiles = [f for f in os.listdir('.') if f.endswith('.json')]
    print(jsonfiles)
    for jsonfile in jsonfiles:
        upload_esr.upload_esr(jsonfile) 
    sys.exit(0) # Done
