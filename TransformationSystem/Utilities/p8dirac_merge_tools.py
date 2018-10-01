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

try:
    dirac = Dirac()
except Exception:
    print('Failed to initialize DIRAC object')
    sys.exit(-9)
    
try:
    fc = FileCatalogClient()
except Exception:
    print('Failed to initialize FileCatalogClient object')
    sys.exist(-9)

def check_lfn_health(lfn):
    status = os.system("source /cvmfs/hep.pnnl.gov/project8/katydid/v2.13.0/setup.sh\nroot -b " + lfn + " -q")
    return status
    #pdb.set_trace()

def concatenate_root_files(output_root_file, input_root_files,force=False):
    '''
    Concatenate the root files into one single root file.
    Doing so will merge the trees of each input file.
    '''
    # Finding hadd and adding force
    command = 'hadd'
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

############################
## Get Merge LFNs  #########
############################
lfn_list = getMergeJobLFNs()
print(lfn_list)
if len(lfn_list) == 0:
    print('No ROOT/HDF5 files found')
    sys.exit(-9)

################################
# Get all lfns based on run_id #
################################
metadata = fc.getFileUserMetadata(lfn_list[0])
#print(metadata)
#pdb.set_trace()
run_id = metadata['Value']['run_id']
software_tag = metadata['Value']['SoftwareVersion']
config_tag = metadata['Value']['ConfigVersion']
meta = {}
meta['run_id'] = run_id
meta['DataExt'] = 'root'
meta['DataFlavor'] = 'event'
verifiedlfnlist = fc.findFilesByMetadata(meta)
verifiedlfnlist = verifiedlfnlist['Value']
print(verifiedlfnlist)

########################
# Check health of LFNs #
########################
verifiedlfnlist = ['rid000006968_6_event.root', 'rid000006968_6_2_event.root', 'rid000006968_6_3_event.root']
lfn_good_list = []
lfn_bad_list = []
for lfn in verifiedlfnlist:
    status = check_lfn_health(lfn)
    if status > 0:
        lfn_good_list.append(lfn)
    else:
        lfn_bad_list.append(lfn)
if len(lfn_good_list) < 1:
    sys.exit(-9)
dirname = os.path.dirname(lfn_good_list[0])
basename = os.path.basename(lfn_good_list[0])
datatype_dir = dirname.replace('data', 'merged')
software_dir = os.path.join(datatype_dir, 'katydid_%s' % software_tag)
config_dir = os.path.join(software_dir, 'termite_%s' % config_tag)

################
# Merge (hadd) #
################
output_filename = 'events_%09d_concat.root' % (run_id)
print('p8dirac_postprocessing: postprocessing.concatenate_root_files({},...)'.format(output_filename))
hadd_status = concatenate_root_files(output_filename, lfn_good_list, force=True)
if hadd_status == 0:
    print('postprocessing: hadd done\n')
else:
    print('hadd failed to create %s.' %(output_filename))
###############
# Upload File #
###############
event_lfn = config_dir + '/' + output_filename
event_pfn = os.getcwd() + '/' + output_filename
res = dirac.addFile(event_lfn, event_pfn, PROD_DEST_DATA_SE)
if not res['OK']:
    print('Failed to upload merged file %s to %s.' % (event_pfn, event_lfn))
    sys.exit(-9)

###################
# Change metadata #
###################
metadata['DataLevel'] = 'merged'
res = fc.setMetadata(datatype_dir, metadata)
if not res['OK']:
    print('Failed to register metadata to LFN %s: %s' % (datatype_dir, metadata))
    sys.exit(-9)

####################
# Update Ancestory #
####################
event_lfn = config_dir + '/' + output_filename
ancestry_dict = {}
ancestry_dict[event_lfn] = {'Ancestors': lfn_good_list}
res = fc.addFileAncestors(ancestry_dict)
if not res['OK']:
    print('Failed to register ancestors: %s' % res['Message'])
    sys.exit(-9)
