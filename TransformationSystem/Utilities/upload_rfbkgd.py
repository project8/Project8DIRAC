#! /usr/bin/env python

#import argparse
import subprocess
from datetime import datetime
import os
import pdb
from pprint import pprint
import sys
import json

import p8dirac_safe_access

from DIRAC.Core.Base import Script
Script.parseCommandLine()

from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
fcc = FileCatalogClient()


OutputSE = 'PNNL-PIC-SRM-SE'


def upload_rfbkgd():
    with open('plot_config.txt') as json_file:
        data = json.load(json_file)
    input_lfns = data['input_lfns']
    #output_pfn = data['input_file']    
    ancestry_dict = {}
    pdf_list = []
    for file in os.listdir(os.getcwd()):
        if file.endswith(".pdf"):
            pdf_list.append(file)
    for file in pdf_list:
        input_lfn = os.path.dirname(input_lfns) + os.sep + file
        output_lfn = input_lfn.replace('raw', 'proc') 
        output_pfn = file
        print(output_lfn)
        print(output_pfn)
        status = p8dirac_safe_access.p8dirac_safe_access('replace', output_lfn, output_pfn, OutputSE, 5, 30)
        print(status)
        if status['OK']:
            ancestry_dict[output_lfn] = {'Ancestors': input_lfns}
            metadata = {'DataFlavor': 'rf_bkgd'}
            print(ancestry_dict)
            # register metadata
            res = fcc.setMetadata(output_lfn, metadata)
            if not res['OK']:
                print(res)

            # register ancestry - so far we just made a dictionary, this
            # actually registers it in the catalog.
            status = fcc.addFileAncestors(ancestry_dict)
            if not status['OK']:
                print('Failed to register descendants of {}.\n'.format(input_lfn))
        else:
            print('Failed to add file {}.\n'.format(output_lfn))

'''
if __name__ == "__main__":
    
    upload_esr(args.timestamp)
    
    upload_esr(sys.argv[1])
'''
