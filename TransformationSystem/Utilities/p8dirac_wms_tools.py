#!/usr/bin/env python

from DIRAC.Core.Base import Script
Script.parseCommandLine()

from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC import gLogger, gConfig, S_OK, S_ERROR

import os
import sys

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
        lfn = results['Value'][jobID]
    except ValueError:
        print('Failed to extract lfn information')
        sys.exit(-9)

    # Make sure there's only 1 LFN
    if not len(lfn) == 1:
        print('Incorrect number of files')
        sys.exit(-9)

    # Create input lfn/filename
    input_lfn = lfn[0].replace('LFN:', '')
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
    gain_lfn = os.path.join(
            dirname.replace('data', 'ts_prod'),
            'Katydid_%s/Termite_%s/Processed/root' % (software_tag, config_tag),
            basename.replace('.egg', '_gain.root'))
    res = dirac.addFile(gain_lfn, gain_pfn, PROD_DEST_DATA_SE)
    if not res['OK']:
        print('Failed to upload gain file: %s' % res['Message'])
        sys.exit(-9)
    print('Gain file uploaded: %s' % gain_lfn)

    # Add metadata to gain file
    gain_metadata = {
            'DataType': 'Data', 'DataLevel': 'Processed',
            'SoftwareVersion': 'Katydid_%s' % software_tag,
            'ConfigVersion': 'Termite_%s' % config_tag,
            'DataExt': 'root', 'DataFlavor': 'Gain'}
    res = fc.setMetadata(gain_lfn, gain_metadata)
    if not res['OK']:
        print('Failed to register metadata to LFN %s: %s'
                % (gain_lfn, gain_metadata))
        sys.exit(-9)

    # Upload event file
    event_pfn = os.getcwd() + '/TracksAndEvents.root'
    event_lfn = os.path.join(
            dirname.replace('data', 'ts_prod'),
            'Katydid_%s/Termite_%s/Processed/root' % (software_tag, config_tag),
            basename.replace('.egg', '_event.root'))
    res = dirac.addFile(event_lfn, event_pfn, PROD_DEST_DATA_SE)
    if not res['OK']:
        print('Failed to upload event file: %s' % res['Message'])
        sys.exit(-9)
    print('Event file uploaded: %s' % event_lfn)

    # Add metadata to event file
    event_metadata = {
            'DataType': 'Data', 'DataLevel': 'Processed',
            'SoftwareVersion': 'Katydid_%s' % software_tag,
            'ConfigVersion': 'Termite_%s' % config_tag,
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
