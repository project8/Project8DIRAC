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
import pdb
import subprocess

ops_dict = Operations().getOptionsDict('Transformations/')
if not ops_dict['OK']:
    print('Failed to get SE information from DIRAC Operation config: %s'
            % ops_dict['Message'])
    sys.exit(-9)

ops_dict = ops_dict['Value']
PROD_DEST_DATA_SE = ops_dict.get('ProdDestDataSE', 'PNNL-PIC-SRM-SE')
PROD_DEST_MONITORING_SE = ops_dict.get('ProdDestMonitoringSE', '')

## TODO: Should use dynamic software tag
def check_lfn_health(pfn):
    #status = os.system("source /cvmfs/hep.pnnl.gov/project8/katydid/" + "v2.13.0" + "/setup.sh\nroot -b " + pfn + " -q")
    status = os.system("source /cvmfs/hep.pnnl.gov/project8/common/current/setup.sh\nroot -b " + pfn + " -q")
    return status

def concatenate_root_files(output_root_file, input_root_files, force=False):
    '''
    Concatenate the root files into one single root file.
    Doing so will merge the trees of each input file.
    '''
    # Finding hadd and adding force
    #command = 'source /cvmfs/hep.pnnl.gov/project8/katydid/v2.13.0/setup.sh\nhadd'
    command = 'source /cvmfs/hep.pnnl.gov/project8/common/current/setup.sh\nhadd'
    if force:
        print('postprocessing: Forcing operation')
        command = '{} -f'.format(command)
    print('postprocessing: command = {}'.format(command))

    command = '{} {}'.format(command,output_root_file)
    print('postprocessing: output = {}'.format(output_root_file))
    if isinstance(input_root_files,str):
        input_root_files = [input_root_files]
    print('postprocessing: input = ')
    for input_file in input_root_files:
        command = '{} {}'.format(command, input_file)
        print(input_file)

    print('postprocessing: Starting hadd')
    status = subprocess.call(command, shell = True)
    #print('postprocessing: hadd done\n')
    return status

def getMergeJobLFNs():
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
    return input_lfns

def uploadJobOutputROOT():
    ############################
    ## Get Merge LFNs  #########
    ############################
    lfn_list = getMergeJobLFNs()
    print(lfn_list)
    
        
    if len(lfn_list) == 0:
        print('No ROOT/HDF5 files found')
        sys.exit(-9)

    try:
        dirac = Dirac()
    except Exception:
        print('Failed to initialize DIRAC object')
        sys.exit(-9)
        
    ################################
    # Get all lfns based on run_id #
    ################################
    try:
        fc = FileCatalogClient()
    except Exception:
        print("Failed to initialize file catalog object.")
        sys.exit(-9)
        
    print(lfn_list)
    metadata = fc.getFileUserMetadata(lfn_list[0])  
    if not metadata['OK']:
        print("problem with metadata query")
        sys.exit(-9)
    print(metadata['Value'])
    
    ########################
    # Check health of LFNs #
    ########################
    lfn_good_list = []
    good_files = []
    bad_files = []
    for lfn in lfn_list:
        local_file = os.path.basename(lfn)
        print('LFN: %s' %lfn)
        print('Local File: %s' % local_file)
        status = check_lfn_health(local_file)
        if status > 0:
            good_files.append(local_file)
            lfn_good_list.append(lfn)
        else:
            print(status)
            bad_files.append(local_file)
    if len(good_files) < 1:
        print("no good files")
        sys.exit(-9)

    ################
    # Merge (hadd) #
    ################
    output_filename = 'events_%09d_merged.root' % metadata['Value']['run_id']
    print('p8dirac_postprocessing: postprocessing.concatenate_root_files({},...)'.format(output_filename))
    hadd_status = concatenate_root_files(output_filename, good_files, force=True)
    if hadd_status == 0:
        print('postprocessing: hadd done\n')
    else:
        print('hadd failed to create %s.' %(output_filename))
        sys.exit(-9)
        
    ###############
    # Upload File #
    ###############
    lfn_dirname = os.path.dirname(lfn_list[0])
    event_lfn = lfn_dirname + '/' + output_filename
    event_pfn = os.getcwd() + '/' + output_filename
    res = dirac.addFile(event_lfn, event_pfn, PROD_DEST_DATA_SE)
    if not res['OK']:
        print('Failed to upload merged file %s to %s.' % (event_pfn, event_lfn))
        sys.exit(-9)

    ###################
    # Change metadata #
    ###################
    datatype_metadata = {'DataFlavor':'merged','DataExt': 'root'}
    res = fc.setMetadata(event_lfn, datatype_metadata)   #meta
    if not res['OK']:
        print('Failed to register metadata to LFN %s: %s' % (event_lfn, datatype_metadata))
        sys.exit(-9)

    ####################
    # Update Ancestory #
    ####################
    ancestry_dict = {}
    ancestry_dict[event_lfn] = {'Ancestors': lfn_good_list}
    res = fc.addFileAncestors(ancestry_dict)
    if not res['OK']:
        print('Failed to register ancestors: %s' % res['Message'])
        sys.exit(-9)
    sys.exit(0) # Done
