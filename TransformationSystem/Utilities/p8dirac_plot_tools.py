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

ops_dict = Operations().getOptionsDict('Transformations/')
if not ops_dict['OK']:
    print('Failed to get SE information from DIRAC Operation config: %s'
            % ops_dict['Message'])
    sys.exit(-9)

ops_dict = ops_dict['Value']
PROD_DEST_DATA_SE = ops_dict.get('ProdDestDataSE', 'PNNL-PIC-SRM-SE')
PROD_DEST_MONITORING_SE = ops_dict.get('ProdDestMonitoringSE', '')

########################
# Check health of LFNs #
########################
def check_lfn_health():
    with open('plot_config.txt') as f:
        json_data = json.load(f)
    lfn = json_data['input_lfns']
    #print('Checking health of merged root file:%s' %lfn)
    input_lfns = lfn
    localfile = os.path.basename(lfn)
    #print('LFN: %s' %lfn)
    #print('Local File: %s' % localfile)
    status = os.system("source /cvmfs/hep.pnnl.gov/project8/common/v0.4.0/setup.sh\nroot -b " + localfile + " -q")
    if not status > 0:
        print('File is not good')
        sys.exit(-9)
    dirname = os.path.dirname(lfn[0])
    basename = os.path.basename(lfn[0])
    exDict = {'status': status, 'input_lfns': input_lfns, 'input_file': os.path.basename(input_lfns)}
    with open('plot_config.txt', 'w') as file:
        file.write(json.dumps(exDict))
    return status   

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
        print('No ROOT/HDF5 files found')
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
    with open('plot_config.txt') as f:
        json_data = json.load(f)
    #print(json_data)
    dirac = Dirac()
    fc = FileCatalogClient()
    ###############
    # Upload Files #
    ###############
    list_pfn_plots = []
    for file in os.listdir(os.getcwd()):
        if file.endswith(".pdf"):
            list_pfn_plots.append(file)
    lfn_list = str(json_data['input_lfns'])
    #print('lfn_list from the config file is %s.' % lfn_list)
    lfn_dirname = os.path.dirname(lfn_list)
    for file in list_pfn_plots:
        event_lfn = lfn_dirname + '/' + file
        event_pfn = os.getcwd() + '/' + file
        #event_lfn = event_lfn.replace('events_', 'rid')
        event_lfn = event_lfn.replace('_merged', '')
        #print('LFN: %s.' %event_lfn)
        # Remove file first if it exists
        res = dirac.removeFile(event_lfn)
        if not res['OK']:
            print('Failed to remove plot file %s.' % (event_lfn))
        res = dirac.addFile(event_lfn, event_pfn, PROD_DEST_DATA_SE)
        if not res['OK']:
            print('Failed to upload plot file %s to %s.' % (event_pfn, event_lfn))
            sys.exit(-9)
        ###################
        # Change metadata #
        ###################
        datatype_metadata = {'DataFlavor':'plot','DataExt': 'pdf'}
        res = fc.setMetadata(event_lfn, datatype_metadata)   #meta
        if not res['OK']:
            print('Failed to register metadata to LFN %s: %s' % (event_lfn, datatype_metadata))
            sys.exit(-9)

        ####################
        # Update Ancestory #
        ####################
        #print('LFN to update Ancestory: %s.' %lfn_list)
        ancestry_dict = {}
        ancestry_dict[event_lfn] = {'Ancestors': lfn_list}
        res = fc.addFileAncestors(ancestry_dict)
        if not res['OK']:
            print('Failed to register ancestors: %s' % res['Message'])
            sys.exit(-9)
    sys.exit(0) # Done
