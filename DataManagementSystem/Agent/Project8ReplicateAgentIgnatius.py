########################################################################
# $HeadURL$
# File: Project8ReplicateAgent.py
# Author: Malachi.Schram, Brent.VanDevender and Vikas.Bansal
########################################################################
""" :mod: Project8ReplicateAgentIgnatiusAgent
    ====================
"""

# # imports
import re
import sys
import tempfile
import datetime
import commands
import os
import urllib
import time
from pprint import pprint
from DIRAC.Core.Utilities.Grid import executeGridCommand
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Resources.Storage.StorageElement import StorageElement
from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin

# Copy from dirac script
from DIRAC import gConfig, gLogger
from DIRAC.ResourceStatusSystem.Client.ResourceStatus import ResourceStatus
from DIRAC.Core.Utilities.PrettyPrint import printTable
from DIRAC.DataManagementSystem.Client.DataManager import DataManager
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.File import File
from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient

__RCSID__ = ' (Fri Oct  2 13:25:08 PDT 2015)  Malachi Schram <malachi.schram@pnnl.gov '

class Project8ReplicateAgentIgnatius(AgentModule):

    """
    .. class:: Project8ReplicateAgentIgnatius 
    """

    def initialize(self):
        """ agent's initalisation

        :param self: self reference
        """
        gLogger.info('Initialize')
        self.dataManager = DataManager()
        self.am_setOption('shifterProxy', 'DataManager')
        self.CopyToSE  = (self.am_getOption("CopyToSE",'PNNL-DIPS-SE'))
        self.SEDataDirPath = (self.am_getOption("SEDataDirPath",'/project8/dirac/data/'))
        self.LocalDataDirPath = (self.am_getOption("LocalDataDirPath",'/data_ignatius/'))
        self.DIRACCfgSEPath = 'Resources/StorageElements'

        return S_OK()

    def __submitRMSOp(self, target_se, lfns_chunk_dict, whichRMSOp='ReplicateAndRegister' ):
        """ target_se : SE name to which to replicate
            lfns_chunk_dict : LFNS dict with 100 lfns as key andeEach lfn has 'Size', 'Checksum'
            whichRMSOp: Choose from RMP operation - ReplicateAndRegister, ReplicateAndRemove, PutAndRegister
            """
    
        ## Setup request
        request = Request()
        request.RequestName = "DDM_"+ str(target_se) +  datetime.datetime.now().strftime("_%Y%m%d_%H%M%S")
        myOp = Operation()
        myOp.Type = whichRMSOp
        myOp.TargetSE = target_se
        ## Add LFNS to operations
        for lfn in lfns_chunk_dict.keys():
            opFile = File()
            opFile.LFN = lfn
            opFile.Size = lfns_chunk_dict[lfn]['Size']
            if "Checksum" in lfns_chunk_dict[lfn]:
                opFile.Checksum = lfns_chunk_dict[lfn]['Checksum']
                opFile.ChecksumType = 'ADLER32'
                ## Add file to operation
                myOp.addFile( opFile )
    
        request.addOperation( myOp )
        reqClient = ReqClient()
        putRequest = reqClient.putRequest( request )
        if not putRequest["OK"]:
            gLogger.error( "Unable to put request '%s': %s" % ( request.RequestName, putRequest["Message"] ) )
            return S_ERROR("Problem submitting to RMS.")


    def execute(self):
        """ execution in one agent's cycle

        :param self: self reference
        """
        
        # Check if CopyToSE is valid
        if not self.CopyToSE.lower() == 'none':
            # Get allowed SEs
            res = gConfig.getSections(self.DIRACCfgSEPath)
            if not res['OK']:
                gLogger.warn('Could not retrieve SE info from DIRAC cfg path %s' %self.DIRACCfgSEPath)
            if res['OK'] and res['Value']:
                if self.CopyToSE not in res['Value']:
                    gLogger.error('Could not find CopyToSE - %s - in DIRAC cfg' %self.CopyToSE)
                    return S_ERROR('CopyToSE %s is not valid' %self.CopyToSE)


        dest_se = self.CopyToSE
        se_data_dir = self.SEDataDirPath
        local_data_dir = self.LocalDataDirPath
        
        for rootdir, subdirs, filenames in os.walk(local_data_dir):
            for filename in filenames:
                if filename.endswith('.mat') or filename.endswith('.MAT') or filename.endswith('.egg') or filename.endswith('_meta.json') or filename.endswith('.msk') or filename.endswith('.Setup'):
                    gLogger.info('Matched local file: ' + filename)
                    pfn = os.path.join(rootdir, filename)
                    lfn = os.path.join(se_data_dir, os.path.join(rootdir, filename).split(local_data_dir)[-1])

                    ### Check if file already exists ###
                    cmd = 'dirac-dms-lfn-accessURL ' + lfn + ' ' + dest_se
                    gLogger.debug(cmd)
                    status, output = commands.getstatusoutput(cmd)
                    if "No such file" not in output:
                        gLogger.info('File already exists ... removing.')
                        cmd = '/bin/rm ' + pfn + ' <<< y'
                        gLogger.debug(cmd)
                        status, output = commands.getstatusoutput(cmd)
                        if status==0:
                            gLogger.info('File successfully removed.')
                        else:
                            gLogger.error('Problem removing file!  rm returned {}'.format(status))
                            gLogger.info('rm cmd was: "{}"'.format(cmd))
                        continue
                    else:   
                    ### Upload file ###
                        cmd = 'dirac-dms-add-file -ddd -o /Resources/Sites/Test=true '
                        gLogger.info("local file is {}".format(lfn))
                        cmd += lfn + ' '
                        gLogger.info("will move file to {}".format(pfn))
                        cmd += pfn + ' ' 
                        cmd += dest_se 
                        gLogger.info("full upload command is:\n{}".format(cmd))
                        initialTime = time.time()
                        status, output = commands.getstatusoutput(cmd)
                        elapsedTime = time.time() - initialTime
                        if status==0:
                            gLogger.info('Upload successful in {} s. Removing local file...'.format(elapsedTime))
                            cmd = '/bin/rm ' + pfn + ' <<< y'
                            gLogger.debug(cmd)
                            status, output = commands.getstatusoutput(cmd)
                            if status==0:
                                gLogger.info('File successfully removed.')
                            else:
                                gLogger.error('Problem removing file!  rm returned {}'.format(status))
                                gLogger.info('rm cmd was: "{}"'.format(cmd))
                        else:
                            gLogger.error('Failed to upload file ' + lfn)

                ### Do metadata for the dir
                if filename.endswith('_meta.json'):
                    meta_json_string = open(filename).read()
                    meta_python_unicode_dict = json.loads(meta_json_string) # converts to unicode
                    
                    meta_python_dict = {} # this one is utf encoded
                    for item in meta_python_unicode_dict.items():
                        key = item[0].encode('utf-8')
                        if item[1] is None:
                            value = 'null'
                        elif isinstance(item[1], (int, float)):
                            value = item[1]
                        elif item[1].isdigit():
                            value = int(item[1].encode('utf-8')) # encode '0' and '1' as int
                        else:
                            value = item[1].encode('utf-8')
                        meta_python_dict[key] = value
                    gLogger.info('Meta Data from file (%s) is: %s' %(filename, meta_python_dict))

        return S_OK()
