#! /usr/bin/env python

#import argparse
import subprocess
from datetime import datetime
import os
import pdb
from pprint import pprint
import sys

import p8dirac_safe_access

from DIRAC.Core.Base import Script
Script.parseCommandLine()

from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
fcc = FileCatalogClient()


OutputSE = 'PNNL-PIC-SRM-SE'


def upload_esr(timestamp):
    #pdb.set_trace()
    year_str   = timestamp[0:4]
    month_str  = timestamp[4:6]
    day_str    = timestamp[6:8]
    hour_str   = timestamp[9:11]
    minute_str = timestamp[11:13]
    second_str = timestamp[13:15]

    # earliest data had on YYYYMMDD_HHMM (no SS), so
    # handle as special case.
    if len(second_str) == 0:
        no_seconds = True
        second_str = '00'
    else:
        no_seconds = False

    year_int   = int(year_str)
    month_int  = int(month_str)
    day_int    = int(day_str)
    hour_int   = int(hour_str)
    minute_int = int(minute_str)
    second_int = int(second_str)
    this_esr_time = datetime(year_int, month_int, day_int, hour_int, minute_int, second_int)

    # before Nov 7, 2016, the ESR data was in .root format.  This code will correctly compute input/output
    # file names and submit the job, but the actual esr.py analysis code called will need to be edited (I think)
    # to handle .root files.
    nov_7_2016_at_midnight = datetime(2016, 11, 7, 00, 00, 00)


    # compute the input and output filenames and paths.
    if no_seconds:
        basename = '{}{}{}_{}{}'.format(year_str, month_str, day_str, hour_str, minute_str)
    else:
        basename = '{}{}{}_{}{}{}'.format(year_str, month_str, day_str, hour_str, minute_str, second_str)

    if this_esr_time < nov_7_2016_at_midnight:
        input_filename = basename + '-esr.root'
    else:
        input_filename = basename + '-esr.json'

    input_path  = os.path.join('/project8/dirac/calib/esr/raw/{}/{}{}'.format(year_str, month_str, day_str), basename)

    output_path = os.path.join('/project8/dirac/calib/esr/proc/{}/{}{}'.format(year_str, month_str, day_str), basename)

    # this is the complete list of POSSIBLE output files.
    # they will not all necessarily exist.
    output_data_list = [
        basename + '-coil1.pdf',
        basename + '-coil2.pdf',
        basename + '-coil3.pdf',
        basename + '-coil4.pdf',
        basename + '-coil5.pdf',
        basename + '-fieldmap.pdf',
        basename + '-plots.root',
        basename + '-result.json'
                        ]

    # a dictionary to record file ancestry
    ancestry_dict = {}
    metadata_dict = {}    
    for output_data in output_data_list:
        if os.path.isfile(output_data):

            output_lfn = os.path.join(output_path, output_data)
            output_pfn = output_data
            input_lfn  = os.path.join(input_path, input_filename)

            status = p8dirac_safe_access.p8dirac_safe_access('replace', output_lfn, output_pfn, OutputSE, 5, 30)
            if status['OK']:
                ancestry_dict[output_lfn] = {'Ancestors': input_lfn}
                metadata_dict[output_lfn] = {'DataFlavor': 'esr'}
                #metadata = {'DataFlavor': 'esr'}
                # register metadata
                #res = fcc.setMetadata(output_lfn, metadata)
                #print(res)
                #if not res['OK']:
                #    print('Could not register metadata.\n')
                
                #    sys.exit(-9)
            else:
                print('Failed to add file {}.\n'.format(output_root_lfn))
                sys.exit(-9)
    # add metadata
    print(metadata_dict)
    status = fcc.setMetadataBulk(metadata_dict)
    if not status['OK']:
        print('Failed to register metadata for {}.\n'.format(input_lfn))
        sys.exit(-9)
    # register ancestry - so far we just made a dictionary, this
    # actually registers it in the catalog.
    print(ancestry_dict)
    status = fcc.addFileAncestors(ancestry_dict)
    if not status['OK']:
        print('Failed to register descendants of {}.\n'.format(input_lfn))
        sys.exit(-9)

    sys.exit(0)
