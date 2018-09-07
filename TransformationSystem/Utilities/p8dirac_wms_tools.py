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
    
    #################################
    ## Create directory structures ##
    #################################
    datatype_dir = dirname.replace('data', 'ts_processed')
    software_dir = os.path.join(datatype_dir, 'katydid_%s' % software_tag)                
    config_dir = os.path.join(software_dir, 'termite_%s' % config_tag)                                  

    ###########################
    ## Upload new data files ##
    ###########################
    
    # Event file
    event_pfn = os.getcwd() + '/TracksAndEvents.root'
    event_lfn = os.path.join(config_dir, basename.replace('.egg', '_event.root'))    
    res = dirac.addFile(event_lfn, event_pfn, PROD_DEST_DATA_SE)
    if not res['OK']:
        print('Failed to upload event file: %s' % res['Message'])
        sys.exit(-9)
    print('Event file uploaded: %s' % event_lfn)
    event_metadata = {'DataExt': 'root', 'DataFlavor': 'event'}
    res = fc.setMetadata(event_lfn, event_metadata)
    if not res['OK']:
        print('Failed to register metadata to LFN %s: %s'
                % (event_lfn, event_metadata))
        sys.exit(-9) 

    # Gain file
    gain_pfn = os.getcwd() + '/GainVariation.root'
    gain_lfn = os.path.join(config_dir, basename.replace('.egg', '_gain.root'))    
    res = dirac.addFile(gain_lfn, gain_pfn, PROD_DEST_DATA_SE)
    if not res['OK']:
        print('Failed to upload gain file: %s' % res['Message'])
        sys.exit(-9)
    print('Gain file uploaded: %s' % gain_lfn)
    gain_metadata = {'DataExt': 'root', 'DataFlavor': 'gain'}
    res = fc.setMetadata(gain_lfn, gain_metadata)
    if not res['OK']:
        print('Failed to register metadata to LFN %s: %s'
                % (gain_lfn, gain_metadata))
        sys.exit(-9) 
    gain_metadata = {'DataExt': 'root', 'DataFlavor': 'gain'}
    res = fc.setMetadata(gain_lfn, gain_metadata)
    if not res['OK']:
        print('Failed to register metadata to LFN %s: %s'
                % (gain_lfn, gain_metadata))
        sys.exit(-9) 
        
    ################################
    ## Setting directory metadata ##
    ################################
   
    # Data Type
    datatype_metadata = {'DataType': 'data', 'DataLevel': 'processed'}
    res = fc.setMetadata(datatype_dir, datatype_metadata)
    if not res['OK']:
        print('Failed to register metadata to LFN %s: %s'
                % (datatype_dir, datatype_metadata))
        sys.exit(-9)  
        
    # All ancestor metadata
    ancestors = fc.getFileAncestors(input_lfn, 1)
    res = fc.getFileUserMetadata(ancestors['Value']['Successful'].keys()[0])    
    if not res['OK']:
        print('Failed to register metadata to LFN %s: %s'
                % (run_id_dir, run_id_metadata))
        sys.exit(-9)  
    metadata_all = res['Value']
    if 'DataLevel' in metadata_all.keys():
        metadata_all.pop('DataLevel')
    res = fc.setMetadata(datatype_dir, metadata_all)                           

    # Software
    software_metadata = {'SoftwareVersion': 'katydid_%s' % software_tag}
    res = fc.setMetadata(software_dir, software_metadata)
    if not res['OK']:
        print('Failed to register metadata to LFN %s: %s'
                % (software_dir, software_metadata))
        sys.exit(-9)                            
        
    # Config
    config_metadata = {'ConfigVersion': 'termite_%s' % config_tag}
    res = fc.setMetadata(config_dir, config_metadata)
    if not res['OK']:
        print('Failed to register metadata to LFN %s: %s'
                % (config_dir, config_metadata))
        sys.exit(-9)  

    #################
    ## Add ancestry ##
    #################
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
def createDetails(software_tag, config_tag):
    lfn_list = getMergeJobLFNs()
    if len(lfn_list) == 0:
        print('No ROOT/HDF5 files found')
        sys.exit(-9)

    # Get all lfns based on run_id
    metadata = fc.getFileUserMetadata(lfn[0])
    run_id = metadata['Value']['run_id']
    meta = {}
    meta['run_id'] = run_id
    verifiedlfnlist = fc.findFilesByMetadata(meta)  
    verifiedlfnlist = verifiedlfnlist['Value']

    # if the job id lfns and run_id lfns list matches, then proceed
    if collections.Counter(verifiedlfnlist) == collections.Counter(lfn_list)
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
            lfn_dirname = lfn_dirname.replace('/ts_processed', '/merged')
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
            version = software_tag               
            '''
            for d in dirs:
                if 'katydid' in d:
                    tmp = d.split('_')
                    version = tmp[1]
                    foundVersion = True
                    break

            if not foundVersion:
                print('LFN %s does not match naming convention')
                sys.exit(-9)
            '''
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
    else:
        print('The list of lfns in the submitted job (JobID - %s) does not match the lfn list for run_id %s', %(int(os.environ['JOBID']), run_id))

