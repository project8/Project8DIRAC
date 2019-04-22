########################################################################
# $HeadURL$
# File: Project8ThreadedReplicateAgentClaude.py
# Author: Vikas.Bansal
# Version : 2
# Rev. Date : April 27, 2017
# This code now submit ESR Calib job
########################################################################
""" :mod: Project8ThreadedReplicateAgentClaude
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
import json
import os.path as path
from multiprocessing import Process
from pprint import pprint

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule import AgentModule

# Copy from dirac script
from DIRAC import gConfig, gLogger
from DIRAC.Core.Utilities.PrettyPrint import printTable
from DIRAC.DataManagementSystem.Client.DataManager import DataManager
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient

from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC.Interfaces.API.Job import Job


__RCSID__ = ' '



class Project8ThreadedReplicateAgentClaude(AgentModule):

    """
    .. class:: Project8ThreadedReplicateAgentClaude
    """

    def initialize(self):
        """ agent's initalisation

        :param self: self reference
        """
        gLogger.info('Initialize')
        self.dataManager = DataManager()
        self.am_setOption('shifterProxy', 'DataManager')
        self.CopyToSE  = (self.am_getOption("CopyToSE",'PNNL-PIC-SRM-SE'))
        self.SEDataDirPath = (self.am_getOption("SEDataDirPath",'/project8/dirac/calib/'))
        self.LocalDataDirPath = (self.am_getOption("LocalDataDirPath",'/data_claude/'))
        self.DIRACCfgSEPath = 'Resources/StorageElements'
        self.dryRun = bool(self.am_getOption("DryRun",True))
        self.fc = FileCatalogClient()
        
        gLogger.info("DryRun: " + str(self.dryRun) )
        gLogger.info("CopyToSE: " + str(self.CopyToSE) )

        ### This defines which calibration directories to consider
        self.calibDirs = ['rf_bkgd', 'esr']
        ### This defines which sub dir under calib dir to replicate
        self.rawDataDir = 'raw'
        self.acceptableFileSuffix = ['-esr.json', '.json', '.dpt', '.root', '.Setup']

        return S_OK()

    
    def _uploadFile(self, dest_se, pfn, lfn, calib_dir):
        """ Private method to upload and register file
            """
        cmd = 'dirac-dms-add-file -o /Resources/Sites/Test=true '
        cmd += lfn + ' '
        cmd += pfn + ' '
        cmd += dest_se
        gLogger.info(cmd)
        initialTime = time.time()
        status, output = commands.getstatusoutput(cmd)
        elapsedTime = time.time() - initialTime
        if status==0:
            gLogger.info('Upload successful in {} s.'.format(elapsedTime))

            ## 20190416 - new code added by Brent to tag metadata at the file level: DataFlavor : [esr,rf_bkgd]
            # tag with metadata
            meta_dict = {'DataFlavor' : calib_dir}
            res = self.fc.setMetadata(lfn, meta_dict)
            if not res['OK']:
                gLogger.error('Setting Metadata on (%s) failed with message (%s)' %(lfn, res['Message']))
            else:
                gLogger.info('Setting meta data on lfn (%s) succeeded' %lfn)
                
        else:
            gLogger.error('Failed to upload file (%s). Status returned (%s) and error returned (%s)' %( lfn, status, output) )

        return status


    def __checkAndRemoveFileOnSE(self, lfn, pfn, dest_se):
        """ Check if a given lfn exist. And if it does remove it physically from the SE
            Output: Return True if file exist and was failed to be removed
            Return False if file was not found or was found but removed.
            """
        ### Check if file already exists ###
        cmd = 'dirac-dms-lfn-accessURL ' + lfn + ' ' + dest_se
        gLogger.debug(cmd)
        status, output = commands.getstatusoutput(cmd)
        if "No such file" not in output:

            gLogger.info('File (%s) already exists ... removing.' %lfn)
            cmd = '/bin/rm ' + pfn + ' <<< y'
            gLogger.debug(cmd)
            status, output = commands.getstatusoutput(cmd)
            if status==0:
                gLogger.info('File (%s) successfully removed.' %pfn)
                return False
            else:
                gLogger.error('Problem removing file!  /bin/rm returned {}'.format(status))
                gLogger.info('rm cmd was: "{}"'.format(cmd))
                return True
        else:
            return False


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
        

        for calib_dir in self.calibDirs:
        
            se_data_dir = path.join(self.SEDataDirPath, path.join(calib_dir,self.rawDataDir))
            local_data_dir = path.join(self.LocalDataDirPath, path.join(calib_dir,self.rawDataDir))
            gLogger.info("Using se dir:"+se_data_dir)
            gLogger.info("Using local dir:"+local_data_dir)

            for currentdir, subdirs, filenames in os.walk(local_data_dir):
                gLogger.debug('In current dir: %s' % currentdir)
                for filename in filenames:
                    if filename.endswith(tuple(self.acceptableFileSuffix)):
                        gLogger.info('Matched local file: ' + filename)
                        pfn = path.join(currentdir, filename)
                        sub_lfn = pfn.split(local_data_dir)[-1].strip("/")
                        lfn = path.join( se_data_dir, sub_lfn )
                        gLogger.debug('pfn/sub_lfn/lfn: %s -- %s -- %s' % (pfn,sub_lfn,lfn))
                        
                        if not self.dryRun:

                            ### Check if file already exists ###
                            if  self.__checkAndRemoveFileOnSE(lfn, pfn, dest_se):
                                continue
                            else:
                                ### Upload file via processes ###
                                status = self._uploadFile(dest_se,pfn,lfn,calib_dir)
        
        return S_OK()
