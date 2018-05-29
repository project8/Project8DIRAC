########################################################################
# $HeadURL$
# File: Project8ThreadedDataReplicateAgent.py
# Author: Vikas.Bansal
# Date: Jan 12, 2018
# Updates: May 29, 2018 (M.G.)
########################################################################
""" :mod: Project8ThreadedDataReplicateAgent
    ====================
"""

# # imports
import re
import sys
import tempfile
import datetime
import commands
import os
import time
import json
import math
import Queue
import gfal2

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Utilities.ThreadPool                            import ThreadPool
from DIRAC.Interfaces.API.Dirac import Dirac

# Copy from dirac script
from DIRAC import gConfig, gLogger
from DIRAC.DataManagementSystem.Client.DataManager import DataManager
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient


__RCSID__ = ' '
#AGENT_NAME = 'DataManagement/Project8ThreadedDataReplicateAgent'

class Project8ThreadedDataReplicateAgent(AgentModule):

    """
    .. class:: Project8ThreadedDataReplicateAgent
    """
    
    # Max number of worker threads by default
    __maxNumberOfThreads = 15

    def initialize(self):
        """ agent's initalisation

        :param self: self reference
        """
        gLogger.info('Initialize')
        self.dataManager = DataManager()
        self.am_setOption('shifterProxy', 'DataManager')
        self.CopyToSE  = (self.am_getOption("CopyToSE",'PNNL-DIPS-SE'))
        self.SEDataDirPath = (self.am_getOption("SEDataDirPath",'/project8/dirac/data/'))
        self.LocalDataDirPaths = (self.am_getOption("LocalDataDirPaths",['/data_ignatius/', '/data_zeppelin']))
        self.MaxFilesToTransferPerCycle = int(self.am_getOption("MaxFilesToTransferPerCycle",200))
        
        self.maxNumberOfThreads = self.am_getOption( 'maxNumberOfThreads', self.__maxNumberOfThreads )
        self.threadPool    = ThreadPool( self.maxNumberOfThreads, self.maxNumberOfThreads )

        # Extra metadata added by the user.
        self.extraMetadatas =  {"DataLevel": "RAW", "DataType": "Data"}
        
        gLogger.info('MaxFilesToTransferPerCycle: ' + str(self.MaxFilesToTransferPerCycle))
        gLogger.info('maxNumberOfThreads: ' + str(self.maxNumberOfThreads))

        self.DIRACCfgSEPath = 'Resources/StorageElements'
        self.fc = FileCatalogClient()
        
        self.acceptableFileSuffix = ['.mat', '.MAT', '.egg', '_meta.json', '.msk', '.Setup', '.json', '_snapshot.json']
        
        return S_OK()


    def __getMetaData(self, filename):
        ### Give json filename as input
        ### return meta data dict
    
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

        return meta_python_dict


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


        #dest_se = self.CopyToSE
        se_data_dir = self.SEDataDirPath
        local_data_dirs = self.LocalDataDirPaths
        gLogger.info("Using SE dir : " + se_data_dir)
        gLogger.info("Using local dir : %s" % ','.join(local_data_dirs))

        # Get files to be copied ( returns a dict with key - lfn and value = local-pfn )
        res_filesToBeCopiedDict = self.getFilesToBeCopied()
        if not res_filesToBeCopiedDict['OK']:
            return res_filesToBeCopiedDict
        
        filesToBeCopiedDict = res_filesToBeCopiedDict['Value']

        gLogger.info("Number of files to be copied in this cycle are : %s" % len(filesToBeCopiedDict))
        while filesToBeCopiedDict:
            
            res_toBeCopied = self.makeFileCopyQueue(filesToBeCopiedDict, self.maxNumberOfThreads)
            if not res_toBeCopied[ 'OK' ]:
                gLogger.error( res_toBeCopied[ 'Message' ] )
                continue ### Go to next iteration of while loop
            self.toBeCopied, lfn_toCopy = res_toBeCopied[ 'Value' ]

            for _x in xrange( self.toBeCopied.qsize() ):
                jobUp = self.threadPool.generateJobAndQueueIt( self._execute )
                if not jobUp[ 'OK' ]:
                    glogger.error( jobUp[ 'Message' ] )
                        
            gLogger.info( 'Blocking until all spawned threads (Num=%s) have finish copying.' %(self.toBeCopied.qsize()) )
            # block until all tasks are done
            self.toBeCopied.join()
            ### Remove all the lfns that are copied in the loop
            for lfn in lfn_toCopy:
                filesToBeCopiedDict.pop(lfn, None)
            gLogger.info( 'All threads are done (Num=%s). Number of files remaining to be copied = %s'
                         %( self.toBeCopied.qsize(), len(filesToBeCopiedDict) ) )

        return S_OK()
                
                
    def removeLocalFile(self, local_pfn):
    
        ### Now remove the file
        try:
            os.remove(local_pfn)
            gLogger.info('File %s successfully removed.' %local_pfn)
        except OSError as e:
            gLogger.error('Problem removing file {} !  remove returned {}'.format(local_pfn, e.strerror))
            pass


    def verifyAndDeleteAlreadyRegisterdFiles(self, fileFC_Dict, fileLocal_Dict ):
        """
            verifyAndDeleteAlreadyRegisterdFiles() checks if the local files are present on the remote SE.
            If they are then local copies are deleted.
            This takes two dictionaries as input: One from FC.getReplicas as returned by Successful key and
            other is is dict with LFN as key and local-PFN as value
            It file is meta data files, then it also verifies if meta data is registered or not.
            """
    
        ### Get gfal2 handle
        gf2 = gfal2.creat_context()
        ### Loop over all files in the input dict
        for lfn in fileFC_Dict:
            
            pfn = fileFC_Dict[lfn][self.CopyToSE]
            try:
                stat_values = gf2.stat(pfn)
                _replica_size = stat_values.st_size
                gLogger.info('File (%s) was found on the SE (%s). Now deleting it locally.' %(lfn, self.CopyToSE))
                if lfn.endswith('_meta.json'):
                    ### Make sure the parent dir has metaData set
                    res = self.registerDirMetaData(lfn,  self.__getMetaData(fileLocal_Dict[lfn]) )
                    ### If setting of meta data failed, then go to next file
                    if not res['OK']: continue
                ###
                self.removeLocalFile(fileLocal_Dict[lfn]) ### Need local PFN as well.
                
            except Exception, err:
                _error_msg = err.message
                msg = 'gfal API to query stat failed on PFN (%s) with output as %s' % (pfn, err)
                gLogger.error(msg)
    


    def registerDirMetaData(self, lfn, meta_dict):
        """
            registerDirMetaData
            registers meta data at dir level as deduced from  provided LFN with the value provided as a dictionary
            """
        filename = os.path.basename(lfn)
        lpn = lfn.split(filename)[0].rstrip('/')
        res = self.fc.setMetadata(lpn, meta_dict)
        if not res['OK']:
            gLogger.error('Setting Meta Data from file (%s) on dir (%s) failed with message (%s)' %(filename, lpn, res['Message']))
        else:
            gLogger.info('Setting meta data on lpn (%s) succeeded' %lpn)
        ### res is either S_OK or S_ERROR
        return res


    def makeFileCopyQueue(self, filesToBeCopiedDict, size):
        """
            makeFileCopyQueue
            This method makes the multi-threaded queue from the files to be copied with max size specified as input.
            It returns the queue and the list of lfns in the queue
            """
        toBeCopied = Queue.Queue()
        lfn_list = []
        ### Add it to the queue to be copied
        for lfn, pfn in filesToBeCopiedDict.items():
            
            ### If size of queue is >= size provided then exit the for loop
            if toBeCopied.qsize() >= size: break
            
            gLogger.info('This local file (%s) will be transferred ' %pfn)
            ### If the file contains meta data then add that info in the queue
            if lfn.endswith('_meta.json'):
                meta_python_dict = self.__getMetaData(pfn)
                toBeCopied.put( {'lfn': lfn, 'pfn': pfn, 'metaData': meta_python_dict} )
                lfn_list.append(lfn)
                gLogger.debug('Meta Data is %s:' %meta_python_dict)
            else:
                toBeCopied.put( {'lfn': lfn, 'pfn': pfn} )
                lfn_list.append(lfn)

        return S_OK( (toBeCopied, lfn_list) )


    def getFilesToBeCopied(self):
        """
            getFilesToBeCopied
            
            This method gets all the files that need to be copied via dirac in one agent cycle.
            """

        local_data_dirs = self.LocalDataDirPaths
        filesToBeCopiedDict = {} ### A dict with LFN as the key and local-PFN as the value for files to be copied
        ### Loop over all directories
        for local_data_dir in local_data_dirs:
            ### Make sure the agent does not copy more than specified files in one cycle. This is good for stopping agent cleanly if need be
            if len(filesToBeCopiedDict) >= self.MaxFilesToTransferPerCycle: break
            ### Do OS walk over local_data_dir (ROACH (.egg) or RSA (.MAT))
            for currentdir, subdirs, filenames in os.walk(local_data_dir):
                ### Make sure the agent does not copy more than specified files in one cycle. This is good for stopping agent cleanly if need be
                if len(filesToBeCopiedDict) >= self.MaxFilesToTransferPerCycle: break
                gLogger.debug('In dir: %s . It has these many files (%s)' % (currentdir, len(filenames)))
                ### Sort file names
                filenames.sort()
                for filename in filenames:
                    gLogger.debug('Found filename: %s' % filename)
                    ### Make sure file ends in acceptable suffix.
                    if not filename.endswith(tuple(self.acceptableFileSuffix)):
                        ### Go to next file
                        continue
                    
                    pfn = os.path.join(currentdir, filename)
                    sub_lfn = pfn.split(local_data_dir)[-1].strip("/")
                    lfn = os.path.join( self.SEDataDirPath, pfn.split(local_data_dir)[-1].strip("/") )
                    gLogger.debug('pfn/sub_lfn/lfn: %s -- %s -- %s' % (pfn,sub_lfn,lfn))
                    filesToBeCopiedDict[lfn] = pfn
                    ### Make sure the agent does not copy more than specified files in one cycle. This is good for stopping agent cleanly if need be
                    if len(filesToBeCopiedDict) >= self.MaxFilesToTransferPerCycle: break


        ### Now We have a queue of files that need to be transferred (if not already transferred)
        gLogger.info('Potentially these many files will be copied in this cycle - %s' % len(filesToBeCopiedDict))
        if len(filesToBeCopiedDict) == 0:
            return S_OK( {} )
        
        ### Lets first check if the files are already in the DFC
        res_FC = self.fc.getReplicas(filesToBeCopiedDict.keys())
        if not res_FC['OK']:
            msg = 'Could not query FC with getReplicas(). Message is : %s' %res_FC['Message']
            gLogger.error(msg)
            return S_ERROR(msg)
        res_FC_Value = res_FC['Value']
        if 'Successful' in res_FC_Value and res_FC_Value['Successful']:
            ### This means that the files are already in the catalog (FC)
            lfns_in_FC = res_FC_Value['Successful'].keys()
            gLogger.warn('Found some files already in the catalog. Will verify and delete them locally. Here is the file dict : %s' %res_FC_Value['Successful'])
            ### verify and delete local copy of files that are already in FC
            localFilesAlreadyCopiedDict = { lfn:filesToBeCopiedDict[lfn] for lfn in res_FC_Value['Successful'] }
            self.verifyAndDeleteAlreadyRegisterdFiles(res_FC_Value['Successful'], localFilesAlreadyCopiedDict)

        lfns_tobeCopied = []
        if 'Failed' in res_FC_Value and res_FC_Value['Failed']:
            ### This means that the files are NOT in the catalog (FC) and can be copied
            lfns_tobeCopied = res_FC_Value['Failed'].keys()
            
        ### Copy only those files that are not in FC
        filesToBeCopiedDict = { lfn:filesToBeCopiedDict[lfn] for lfn in lfns_tobeCopied }
        
        return S_OK( filesToBeCopiedDict )


    # Private methods ............................................................

    def _execute( self ):
        """
        Method run by the thread pool. It enters a loop until there are no files
        on the queue. On each iteration, it copies and then removes the file.
        If there are no more files in the queue, the loop is finished.
        """
    
        while True:
        
            try:
                file = self.toBeCopied.get_nowait()
            except Queue.Empty:
                return S_OK()
                    
            gLogger.verbose( '%s - %s being processed' % ( file[ 'lfn' ], file[ 'pfn' ] ) )

            ### Upload file to SE and register it in DIRAC
            dirac = Dirac()
            initialTime = time.time()
            uploadStatus = dirac.addFile(file[ 'lfn' ], file[ 'pfn' ], self.CopyToSE)
            elapsedTime = time.time() - initialTime
            
            if uploadStatus['OK']:
                gLogger.info('File {} upload took {} s. Now deleting local file.'.format(file[ 'lfn' ], round(elapsedTime,2)))
                
                ### If file has metadata then register it in the respective dir.
                ### It is safe to re-register the meta data
                if 'metaData' in file :
                    # register this metadata
                    if file['metaData']:
                        metaDataDict = file['metaData']
                        metaDataDict.update(self.extraMetadatas)
                        res = self.registerDirMetaData(file[ 'lfn' ], metaDataDict)
                        if not res['OK']:
                            ### If registering meta data failed, then finish the thread gracefully and go to next thread
                            self.toBeCopied.task_done()
                            continue

                    else:
                        gLogger.error('Meta Data for this dir (%s) was not found.' %(lpn))
            
                ### Now remove the file
                self.removeLocalFile(file[ 'pfn' ])
                    
            else:
                gLogger.error('Failed to upload file (%s). Message is (%s)' %(file[ 'lfn' ], uploadStatus['Message']))

            # Used together with join !
            self.toBeCopied.task_done()

    #...............................................................................
    #EOF

