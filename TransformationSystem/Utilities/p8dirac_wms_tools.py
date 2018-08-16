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

def getJobLFN():
    jobID = int(os.environ['JOBID'])

    # Get InputData
    dirac = Dirac()
    res = dirac.getJobInputData(jobID)
    if not res['OK']:
        print('Failed to get job input data: %s' % res['Message'])
        sys.exit(-9)

    # Try to extract the input LFN
    lfns = []
    try:
        lfns = res['Value'][jobID]
    except ValueError:
        print('Failed to extract lfn information')
        sys.exit(-9)

    # Make sure there's only 1 LFN
    if not len(lfns) == 1:
        print('Incorrect number of files')
        sys.exit(-9)

    # Create input lfn/filename
    input_lfn = lfns[0].replace('LFN:', '')
    return input_lfn

def getJobFileName():
    return os.path.basename(getJobLFN())

def uploadJobOutputROOT(software_tag, config_tag):
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

    input_lfn = getJobLFN()
    dirname = os.path.dirname(input_lfn)
    basename = os.path.basename(input_lfn)

    # Upload context file. Antonio: does this need to be done?
    #output_cfg_lfn = os.path.join(dirname.replace('data', 'ts_prod'),
    #        'katydid_v2.11.0/cfg', basename.replace('.egg', '.yaml'))
    #output_cfg_pfn = os.getcwd() + '/Katydid_ROACH_Config.yaml'
    #res = dirac.addFile(output_cfg_lfn, output_cfg_pfn, PROD_DEST_DATA_SE)

    # Upload gain file
    gain_pfn = os.getcwd() + '/GainVariation.root'
    tmp = dirname.split('/data/')
    gain_lfn = os.path.join(
            tmp[0],
            'ts_prod',
            'katydid_%s/termite_%s/processed/root' % (software_tag, config_tag),
            tmp[1],
            basename.replace('.egg', '_gain.root'))
    res = dirac.addFile(gain_lfn, gain_pfn, PROD_DEST_DATA_SE)
    if not res['OK']:
        print('Failed to upload gain file: %s' % res['Message'])
        sys.exit(-9)
    print('Gain file uploaded: %s' % gain_lfn)
    
    # Get the run_id
    ancestors = fc.getFileAncestors(input_lfn, 1)
    metadata = fc.getFileUserMetadata(ancestors['Value']['Successful'].keys()[0]) #UserMetadata(lfn)

    if not metadata['OK']:
       print('Failed to retrieve metadata for %s: %s' % (lfn, metadata['Message']))
       continue

    if not metadata['Value'].get('run_id'):
       print('No run_id for %s' % lfn)
       continue
    run_id = metadata['Value']['run_id']

    # Add metadata to gain file
    gain_metadata = {
            'run_id': '%s' % run_id, 'DataType': 'Data', 'DataLevel': 'Processed',
            'SoftwareVersion': 'katydid_%s' % software_tag,
            'ConfigVersion': 'termite_%s' % config_tag,
            'DataExt': 'root', 'DataFlavor': 'Gain'}
    res = fc.setMetadata(gain_lfn, gain_metadata)
    if not res['OK']:
        print('Failed to register metadata to LFN %s: %s'
                % (gain_lfn, gain_metadata))
        sys.exit(-9)

    # Upload event file
    event_pfn = os.getcwd() + '/TracksAndEvents.root'
    event_lfn = os.path.join(
            tmp[0],
            'ts_prod',
            'katydid_%s/termite_%s/processed/root' % (software_tag, config_tag),
            tmp[1],
            basename.replace('.egg', '_event.root'))
    res = dirac.addFile(event_lfn, event_pfn, PROD_DEST_DATA_SE)
    if not res['OK']:
        print('Failed to upload event file: %s' % res['Message'])
        sys.exit(-9)
    print('Event file uploaded: %s' % event_lfn)

    # Get the run_id
    ancestors = fc.getFileAncestors(input_lfn, 1)
    metadata = fc.getFileUserMetadata(ancestors['Value']['Successful'].keys()[0]) #UserMetadata(lfn)

    if not metadata['OK']:
       print('Failed to retrieve metadata for %s: %s' % (lfn, metadata['Message']))
       continue

    if not metadata['Value'].get('run_id'):
       print('No run_id for %s' % lfn)
       continue
    run_id = metadata['Value']['run_id']
    
    # Add metadata to event file
    event_metadata = {
            'run_id': run_id, 'DataType': 'Data', 'DataLevel': 'Processed',
            'SoftwareVersion': 'katydid_%s' % software_tag,
            'ConfigVersion': 'termite_%s' % config_tag,
            'DataExt': 'root', 'DataFlavor': 'Event'}
    res = fc.setMetadata(event_lfn, event_metadata)
    if not res['OK']:
        print('Failed to register metadata to LFN %s: %s'
                % (event_lfn, event_metadata))
        sys.exit(-9)

    # Add ancestry
    ancestry_dict = {}
    ancestry_dict[gain_lfn] = {'Ancestors': input_lfn}
    ancestry_dict[event_lfn] = {'Ancestors': input_lfn}
    res = fc.addFileAncestors(ancestry_dict)
    if not res['OK']:
        print('Failed to register ancestors: %s' % res['Message'])
        sys.exit(-9)

    sys.exit(0) # Done

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

# The idea is to call this function before running p8dirac_postprocessing.py
# in p8dirac_postprocessing.sh. It'll create the json file, which we can then
# pass the path to to p8dirac_postprocessing.py
def createDetails():
    lfn_list = getMergeJobLFNs()
    if len(lfn_list) == 0:
        print('No ROOT/HDF5 files found')
        sys.exit(-9)

    # Code adapted from ladybug/post-processing/job_submitter.py
    file_dict = {}
    for lfn in lfn_list:
        metadata = fc.getFileUserMetadata(lfn)
        if not metadata['OK']:
            print('Failed to retrieve metadata for %s: %s'
                    % (lfn, metadata['Message']))
            continue
        if not metadata['Value'].get('run_id'):
            print('No run_id for %s' % lfn)
            continue

        run_id = metadata['Value']['run_id']
        lfn_dirname = os.path.dirname(lfn)
        # Change the location from Processed to Merged
        lfn_dirname = lfn_dirname.replace('/Processed', '/Merged')
        if lfn_dirname.endswith('/root'):
            lfn_dirname = lfn_dirname.replace('/root', '')
        elif lfn_dirname.endswith('/h5'):
            lfn_dirname = lfn_dirname.replace('/h5', '')
        else:
            print('%s format not supported' % lfn_dirname)
            continue

        # Extract the analysis name
        foundVersion = False
        dirs = lfn_dirname.split('/')
        for d in dirs:
            if 'katydid' in d:
                tmp = d.split('_')
                version = tmp[1]
                foundVersion = True
                break
        if not foundVersion:
            print('LFN %s does not match naming convention')
            sys.exit(-9)
        if not file_dict.get(version):
            # Assumes files have the same run_id (plugin should ensure this)
            file_dict[version] = {
                    'run_id': run_id,
                    'lfn_list': [],
                    'output_lfn_path': lfn_dirname}
        if run_id != file_dict[version]['run_id']:
            print('Warning: run_id %d inconsistent with other job lfn'
                    'run_ids (%d)' % (run_id, file_dict[version]['run_id']))
            continue
        file_dict[version]['lfn_list'].append(lfn)

    fp = open('details.json', 'w')
    json.dump(file_dist, fp)
    fp.close()

    return os.path.join(os.getswd(), 'details.json')

