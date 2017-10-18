#! /usr/bin/env python

# Move data:
# It takes an input file as input containing a dictionary of the folders to move and their new location.
# For each subfolder to move, it will first download the files on the node, then add them to their new location on the FC and finally remove the files from the FC
# NB: this script does not protect the genealogy of the files or the meta-informations (yet...)
# TODO:
#  - add metadata
#  - get ancestors
# Author: M Guigue
# Creation: Oct 10 2017


import argparse
import json
import os

from DIRAC.Interfaces.API.Dirac import Dirac
dirac = Dirac()


from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
fcc = FileCatalogClient()

se = 'PNNL-HEP-SRM-SE'


def main():
    parser = argparse.ArgumentParser(
        description='Move files in the FC (does not preserve ancestry)')
    parser.add_argument(
        'filename', help='Name of the file containing the location of the files to move and where')
    args = parser.parse_args()

    filename = args.filename
    print("Move_Data: reading input file {}".format(filename))
    import yaml
    with open(filename, 'r') as stream:
        try:
            theDict = yaml.load(stream)
        except yaml.YAMLError as exc:
            print(exc)

    for aKey in theDict.keys():
        print("Move_Data: Doing folder {}".format(aKey))
        aDict = theDict[aKey]
        for infile, outfile in aDict.iteritems():
            status = dirac.getFile(infile)
            if not status['OK']:
                print("Move_Data: failed getting file ")
                return
            infilename = os.path.basename(filename)
            if not os.path.exists(infilename):
                print("Move_Data: file does not exist")
                return
            print("Move_Data: Successfully downloaded file")

            status = dirac.addFile(outfile, infilename, se)
            if not status['OK']:
                print("Move_Data: failed uploading file")
                return
            print("Move_Data: Successfully uploaded file")

            status = dirac.removeFile(infile)
            if not status['OK']:
                print("Move_Data: failed removing file on FC")
                return
            print("Move_Data: Successfully removed file on FC")

            os.remove(infilename)
            if os.path.exists(infilename):
                print("Move_Data: file is still here")
                return
            print("Move_Data: Successfully removed local file")
        print('Moved_Data: making sure it is empty first')
        lfnlist = sorted([lfn for lfn in
                          fcc.findFilesByMetadata(
                              {}, path=aKey)['Value']
                          ])
        if lfn.len() != 0:
            print("Move_Data: folder is not empty")
            return
        print("Move_Data: done with this folder -> removing it")
        fcc.removeDir(aKey)
        print("Move_Data: removed directory")


if __name__ == "__main__":
    main()
