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
import upload_rfbkgd

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
        sys.exit(-9)

    # Try to extract the LFNs
    lfns = []
    try:
        lfns = res['Value'][jobID]
    except ValueError:
        print('Failed to extract LFN information')
        sys.exit(-9)

    # Clean up LFN
    input_lfns = [lfn.replace('LFN:', '') for lfn in lfns]
    if len(input_lfns) == 0:
        print('No JSON files found')
        sys.exit(-9)
    
    if len(input_lfns) == 1:
        input_lfns = input_lfns[0]
    else:
        print("Length of lfns is not 1")
        sys.exit(-9)
    #print('Input lfn is: %s' %input_lfns)

    # Write out lfn etc. to a config file
    exDict = {'input_lfns': input_lfns, 'input_file': os.path.basename(input_lfns)}
    with open('plot_config.txt', 'w') as file:
        file.write(json.dumps(exDict))
    return input_lfns

########################################
# Upload and register files with DIRAC #
########################################
def uploadJobOutputROOT():   
    dptfiles = [f for f in os.listdir('.') if f.endswith('.dpt')]
    for dptfile in dptfiles:
        upload_rfbkgd.upload_rfbkgd() 
    sys.exit(0) # Done
