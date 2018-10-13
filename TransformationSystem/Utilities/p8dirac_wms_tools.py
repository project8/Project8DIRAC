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
        #sys.exit(-9)
    if res['OK']:
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
        #sys.exit(-9)
    if res['OK']:
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

