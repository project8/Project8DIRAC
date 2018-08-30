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



def add_file(dest_se, pfn, lfn):
    
    cmd = 'dirac-dms-add-file -o /Resources/Sites/Test=true '
    #gLogger.info("local file is {}".format(lfn))
    cmd += lfn + ' '
    #gLogger.info("will move file to {}".format(pfn))
    cmd += pfn + ' '
    cmd += dest_se
    gLogger.info(cmd)
    #gLogger.info("full upload command is:\n{}".format(cmd))
    initialTime = time.time()
    status, output = commands.getstatusoutput(cmd)
    elapsedTime = time.time() - initialTime
    if status==0:
        gLogger.info('Upload successful in {} s.'.format(elapsedTime))
        ### VB: Let add_file() method not delete files on the local directory. This also effectively removes threading. Should be OK on calib files
        #gLogger.info('Upload successful in {} s. Removing local file...'.format(elapsedTime))
        #cmd = '/bin/rm ' + pfn + ' <<< y'
        #gLogger.debug(cmd)
        #status, output = commands.getstatusoutput(cmd)
        #if status==0:
        #    gLogger.info('File successfully removed.')
        #else:
        #    gLogger.error('Problem removing file!  rm returned {}'.format(status))
        #    gLogger.info('rm cmd was: "{}"'.format(cmd))
    else:
        gLogger.error('Failed to upload file (%s). Status returned (%s) and error returned (%s)' %( lfn, status, output) )


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
        self.CopyToSE  = (self.am_getOption("CopyToSE",'PNNL-DIPS-SE'))
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
        self.ProcessThreshold = 50

        return S_OK()

    def _uploadFile(self, dest_se, pfn, lfn):
        """ Private method to upload and register file
            """
        cmd = 'dirac-dms-add-file -o /Resources/Sites/Test=true '
        #gLogger.info("local file is {}".format(lfn))
        cmd += lfn + ' '
        #gLogger.info("will move file to {}".format(pfn))
        cmd += pfn + ' '
        cmd += dest_se
        gLogger.info(cmd)
        #gLogger.info("full upload command is:\n{}".format(cmd))
        initialTime = time.time()
        status, output = commands.getstatusoutput(cmd)
        elapsedTime = time.time() - initialTime
        if status==0:
            gLogger.info('Upload successful in {} s.'.format(elapsedTime))
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
            ### Lets call WMS job on the file now
            
            ### Skip calling job on rf_bkgd json file
            if not ('rf_bkgd' in lfn and ( lfn.endswith('.json') or lfn.endswith('.Setup') )):
                resJob = self._submitJob(lfn)
                if not resJob['OK']:
                    gLogger.error('Job Submission failed. Msg: %s' %resJob['Message'])
                    return True
            else:
                gLogger.info('No WMS job to be submitted on this lfn (%s)' %lfn)
            
            ### Job submission was successful. Continue to remove the inputFile
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


    def _getOneProcLFN(self, lfn, CalibType):
        """ This private method gets the LFN of one of processed file from WMS job
            """
        
        _ProcDirName = 'proc'
        _outLFN = None
        
        _resultFileKind = None
        _splitOnChar = None
        if CalibType == 'esr':
            _resultFileKind = 'result.json'
            _splitOnChar = '-'
        elif CalibType == 'rf_bkgd':
            _resultFileKind = 'pdf'
            _splitOnChar = '.'
        else:
            gLogger.error("Cannot determine calibration type")
        
        if CalibType in lfn:
            
            _prepandPath = path.dirname(lfn)
            _prepandPath = _prepandPath.replace( self.rawDataDir, _ProcDirName )
            _fileName = path.basename(lfn)
            _filePrefix = _fileName.split(_splitOnChar)[0]
            _procFileName = _filePrefix + _splitOnChar + _resultFileKind
            _outLFN = path.join(_prepandPath, _procFileName)

        return _outLFN
        

    def _submitJob(self, inputLFN):
        """ This private method submits a WMS job
            """

        ### Define job parameter values here
        _site = 'DIRAC.PNNL.us'
        _cputime = 1000
        _outputSE = 'PNNL-PIC-SRM-SE'
        ### For ESR
        _sh_script =     '/opt/dirac/pro/DIRAC/DataManagementSystem/Agent/esr_scripts/esr.sh'
        _py_script =     '/opt/dirac/pro/DIRAC/DataManagementSystem/Agent/esr_scripts/esr.py'
        _upload_script = '/opt/dirac/pro/DIRAC/DataManagementSystem/Agent/esr_scripts/upload_esr.py'
        _access_script = '/opt/dirac/pro/DIRAC/DataManagementSystem/Agent/esr_scripts/p8dirac_safe_access.py'
        #_outputFiles = ['*result.json', '*plots.root', '*.png', '*.pdf']
        _arguments = '*.json'
        ############################
    
        ## Submit Job only if it was not done before
        
        if 'esr' in inputLFN:
            _calibType =  'esr'
        elif 'rf_bkgd' in inputLFN:
            _calibType =  'rf_bkgd'
            #_outputFiles = ['*.pdf', '*.png']
            _arguments = '*.dpt'
            _sh_script = '/opt/dirac/pro/DIRAC/DataManagementSystem/Agent/rf_bkgd_scripts/rf_bkgd.sh'
            _py_script = '/opt/dirac/pro/DIRAC/DataManagementSystem/Agent/rf_bkgd_scripts/rf_bkgd.py'
        else:
            gLogger.error('Cannot find calibration type')
            _calibType = 'NotAvailable'

        ### Get name(LFN) of one of the processed file
        procFileName = self._getOneProcLFN( inputLFN, _calibType )
        if not procFileName:
            msg = 'Output file name was not reconstructed from input LFN (%s). Exiting ' %inputLFN
            gLogger.error(msg)
            return S_ERROR(msg)

        ### Check if it exists
        cmd = 'dirac-dms-lfn-accessURL ' + procFileName + ' ' + _outputSE
        gLogger.debug(cmd)
        status, output = commands.getstatusoutput(cmd)
        if "No such file" not in output:
            gLogger.info('OutputFile (%s) already exists. No more WMS job to be submitted.' %procFileName)
            return S_OK()
        
        _inputFile = path.basename(inputLFN)
        _outputPath = path.dirname(procFileName)
        
        ### Define Job parameters
        job = Job()
        job.setName('{}: {}'.format(_calibType, _inputFile))
        job.setDestination(_site)
        job.setCPUTime(_cputime)
        job.addToOutputSandbox.append('std.err')
        job.addToOutputSandbox.append('std.out')
        if _calibType == 'esr':
            job.setInputSandbox([_sh_script, _py_script, _upload_script, _access_script])
        else
            job.setInputSandbox([_sh_script, _py_script])
            job.setOutputData(_outputFiles, _outputSE, _outputPath )
        _inputLFN = 'LFN:' + inputLFN
        job.setInputData(_inputLFN)
        job.setExecutable(_sh_script, arguments=_arguments)
        
        
        ### Now ready to submit job
        dirac = Dirac()
        res = dirac.submit(job)
        if not res['OK']:
            msg = 'Problem submitting job on inputFile (%s). Message is %s' %(inputLFN, res['Message'])
            gLogger.error(msg)
            return S_ERROR(msg)
        
        gLogger.info('Job (%s) successfully submitted on inputFile (%s)' % (res['Value'], inputLFN))
        return S_OK()



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
        

        numProc=0
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
                            #if numProc >= self.ProcessThreshold:
                                #pmesg = 'Too many active process (%s). Sleeping for 5 sec' %self.ProcessThreshold
                                #gLogger.info(pmesg)
                                #time.sleep(5)
                                #numProc=0

                            ### Check if file already exists ###
                            if  self.__checkAndRemoveFileOnSE(lfn, pfn, dest_se):
                                continue
                            else:
                                ### Upload file via processes ###
                                ### All other files besides meta data file
                                #pmesg = 'Submitting process #%s.' % numProc
                                #gLogger.info(pmesg)
                                #p = Process(target=add_file,args=(dest_se,pfn,lfn,))
                                #p.start()
                                #numProc+=1
                                status = self._uploadFile(dest_se,pfn,lfn)
        
        return S_OK()
