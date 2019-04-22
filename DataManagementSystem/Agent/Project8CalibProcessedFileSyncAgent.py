########################################################################
# $HeadURL$
# File: Project8CalibProcessedFileSyncAgent.py
# Author: Vikas.Bansal
# Version : 1
# First Date : April 27, 2017
# Usage: To sync back output files from jobs that processed calib files
########################################################################
""" :mod: Project8CalibProcessedFileSyncAgent
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


__RCSID__ = ' '


class Project8CalibProcessedFileSyncAgent(AgentModule):

    """
    .. class:: Project8CalibProcessedFileSyncAgent
    """

    def initialize(self):
        """ agent's initalisation

        :param self: self reference
        """
        gLogger.info('Initialize')
        self.dataManager = DataManager()
        self.am_setOption('shifterProxy', 'DataManager')
        
        self.SEDataDirPath = (self.am_getOption("SEDataDirPath",'/project8/dirac/calib'))
        self.LocalDataDirPath = (self.am_getOption("LocalDataDirPath",'/data_claude/'))
        self.DIRACCfgSEPath = 'Resources/StorageElements'
        self.fc = FileCatalogClient()
        
        #gLogger.info("DryRun: " + str(self.dryRun) )
        #gLogger.info("CopyToSE: " + str(self.CopyToSE) )

        ### This defines which calibration directories to consider
        self.calibDirs = ['rf_bkgd', 'esr']
        self.ProcDataDir = 'proc'

        return S_OK()

    def _syncDir(self, LPN, localDir):
        """ Private method to upload and register file
            """
        cmd = 'dirac-dms-directory-sync -o /Resources/Sites/Test=true '
        cmd += LPN + ' ' + localDir
        gLogger.info(cmd)
        initialTime = time.time()
        status, output = commands.getstatusoutput(cmd)
        elapsedTime = time.time() - initialTime
        if status==0:
            gLogger.info('Sync for {} successful in {} s.'.format(LPN, elapsedTime))
        else:
            gLogger.error('Failed to sync (%s). Status returned (%s) and error returned (%s)' %( LPN, status, output) )

        return status


    def execute(self):
        """ execution in one agent's cycle

        :param self: self reference
        """
 
        for calib_dir in self.calibDirs:

            gLogger.info("Syncing {}".format(calib_dir))
            
            se_data_dir = path.join(self.SEDataDirPath, path.join(calib_dir,self.ProcDataDir))
            local_data_dir = path.join(self.LocalDataDirPath, path.join(calib_dir,self.ProcDataDir))
            
            gLogger.info("Using se dir:"+se_data_dir)
            gLogger.info("Using local dir:"+local_data_dir)

            status = self._syncDir( se_data_dir, local_data_dir)
            
        
        return S_OK()
