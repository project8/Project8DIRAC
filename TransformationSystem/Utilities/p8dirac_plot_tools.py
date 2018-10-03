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

def check_lfn_health(lfn, software_tag):
    status = os.system("source /cvmfs/hep.pnnl.gov/project8/katydid/" + software_tag + "/setup.sh\nroot -b " + lfn + " -q")
    return status
  
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
    return input_lfns
  
def p8dirac_quality_plots(rootfile):
    input_root_filename = os.path.basename(rootfile)
    where_in_catalog = os.path.dirname(rootfile)
    print('p8dirac_postprocessing: Looking for {}'.format(input_root_filename))
    if not rootfile.endswith('.root'):
        print('p8dirac_postprocessing: {} is not a root file. Stopping here!'.format(
            rootfile))
        return
    print('p8dirac_postprocessing: Starting generation of quality plots')
    list_pfn_plots = postprocessing.quality_plots(
        input_root_filename, 'multiTrackEvents')

    print('p8dirac_postprocessing: listing file in current folder:')
    files = [f for f in os.listdir('.') if os.path.isfile(f)]
    for f in files:
        print(f)

    for f in list_pfn_plots:
        output_lfn_name = os.path.join(os.path.abspath(
            os.path.join(where_in_catalog, os.pardir)), f)
        print('p8dirac_postprocessing: replacing file {} as {}'.format(
            f, output_lfn_name))
        status = p8dirac_safe_access.p8dirac_safe_access(
            'replace', output_lfn_name, f, SE, 3, 30)
        if status['OK']:
            print('p8dirac_postprocessing: successfully added {} as {}'.format(
                f, output_lfn_name))

        print('p8dirac_postprocessing: adding ancestors')
        ancestry_dict = {'{}'.format(output_lfn_name): {'Ancestors': rootfile}}
        status = fcc.addFileAncestors(ancestry_dict)
        if not status['OK']:
            print('p8dirac_postprocessing: Failed to register ancestry {} for {}.'.format(
                rootfile, output_lfn_name))
            return

    print("p8dirac_postprocessing: control plots created and added to the catalog")
    return

def uploadJobOutputRoot():
    ############################
    ## Get Merge LFNs  #########
    ############################
    lfn_list = getPlotJobLFNs()
    print(lfn_list)
    if len(lfn_list) == 0:
        print('No ROOT/HDF5 files found')
        sys.exit(-9)

    ################################
    # Get all lfns based on run_id #
    ################################
    try:
        fc = FileCatalogClient()
    except Exception:
        print("Failed to initialize file catalog object.")
        sys.exit(-9)
    metadata = fc.getFileUserMetadata(lfn_list[0])
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
    lfn_good_list = []
    lfn_bad_list = []
    for lfn in verifiedlfnlist:
        status = check_lfn_health(lfn, software_tag)
        if status > 0:
            lfn_good_list.append(lfn)
        else:
            lfn_bad_list.append(lfn)
    if len(lfn_good_list) < 1:
        sys.exit(-9)
    dirname = os.path.dirname(lfn_good_list[0])
    basename = os.path.basename(lfn_good_list[0])
    datatype_dir = dirname.replace('ts_processed', 'merged')
    software_dir = os.path.join(datatype_dir, 'katydid_%s' % software_tag)
    config_dir = os.path.join(software_dir, 'termite_%s' % config_tag)

    ################
    # Plot #
    ################
    #output_filename = 'events_%09d_merged.root' % (run_id)
    #print('p8dirac_postprocessing: postprocessing.concatenate_root_files({},...)'.format(output_filename))
    #hadd_status = concatenate_root_files(output_filename, lfn_good_list, force=True)
    #if hadd_status == 0:
    #    print('postprocessing: hadd done\n')
    #else:
    #    print('hadd failed to create %s.' %(output_filename))
        
    ###############
    # Upload File #
    ###############
    event_lfn = config_dir + '/' + output_filename
    event_pfn = os.getcwd() + '/' + output_filename
    res = dirac.addFile(event_lfn, event_pfn, PROD_DEST_DATA_SE)
    if not res['OK']:
        print('Failed to upload plot file %s to %s.' % (event_pfn, event_lfn))
        sys.exit(-9)

    ###################
    # Change metadata #
    ###################
    metadata['DataLevel'] = 'plots'
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

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description='Create concatenate file from root and h5 files and quality plots')
    parser.add_argument('json_file', help='A config file (.yaml or .json)')
    args = parser.parse_args()

    if os.path.exists(args.json_file):
        with open(args.json_file, 'r') as f:
            details = json.load(f)
        print(details)
        rootfilepath = p8dirac_concat(details=details, fileformat='root')
        # p8dirac_concat(details=details, fileformat='h5')
        p8dirac_quality_plots(rootfilepath)
    else:
        print('p8dirac_postprocessing: {} does not exist'.format(args.json_file))
    print("p8dirac_postprocessing: complete! Good Job!")
