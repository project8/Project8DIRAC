"""  TransformationPlugin is a class wrapping the supported transformation plugins
"""

import random
import time
import os

from DIRAC                              import gConfig, S_OK, S_ERROR
from DIRAC.Core.Utilities.SiteSEMapping import getSitesForSE, getSEsForSite
from DIRAC.Core.Utilities.List          import breakListIntoChunks

from DIRAC.Resources.Catalog.FileCatalog  import FileCatalog
from DIRAC.DataManagementSystem.Client.DataManager import DataManager
from DIRAC.TransformationSystem.Client.PluginBase import PluginBase
from DIRAC.TransformationSystem.Client.Utilities import PluginUtilities, getFileGroups
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient

from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations

__RCSID__ = "49ba04b (2016-12-07 14:20:17 +0100) Federico Stagni <federico.stagni@cern.ch>"


class TransformationPlugin( PluginBase ):
  """ A TransformationPlugin object should be instantiated by every transformation.
  """

  def __init__( self, plugin, transClient = None, dataManager = None, fc = None ):
    """ plugin name has to be passed in: it will then be executed as one of the functions below, e.g.
        plugin = 'BySize' will execute TransformationPlugin('BySize')._BySize()
    """
    super( TransformationPlugin, self ).__init__( plugin )

    self.data = {}
    self.files = False
    self.startTime = time.time()
    self.opsHelper = Operations()

    if transClient is None:
      transClient = TransformationClient()

    if dataManager is None:
      dataManager = DataManager()

    if fc is None:
      self.fc = FileCatalog()

    self.util = PluginUtilities( plugin,
                                 transClient = transClient,
                                 dataManager = dataManager,
                                 fc = self.fc )

  def __del__( self ):
    self.util.logInfo( "Execution finished, timing: %.3f seconds" % ( time.time() - self.startTime ) )

  def isOK( self ):
    self.valid = True
    if ( not self.data ) or ( not self.params ):
      self.valid = False
    return self.valid

  def setParameters( self, params ):
    """ Need to pass parameters also to self.util
    """
    self.params = params
    self.util.setParameters(params)

  def setInputData( self, data ):
    self.data = data
    self.util.logDebug( "Set data: %s" % self.data )

  def setTransformationFiles( self, files ):  # TODO ADDED
    self.files = files

  def _Standard( self ):
    """ Simply group by replica location (if any)
    """
    return self.util.groupByReplicas( self.data, self.params['Status'] )

  def _P8FilterForRAWData( self):
    """
    Generate a task for Project8 raw data
    """
    print('\t MS::Running P8FilterForRAWData')
    filtered_data = {k:v for (k,v) in self.data.items() if '.egg' in k}
    #print('MT:: _P8FilterForRAWData filtered_data ' + str(self.data.items()))
    return self.util.groupBySize( filtered_data, self.params['Status'] )

  def _P8Merge(self):
    """
    Generate a task that merges processed data
    """
    print('\t MS::Running P8Merge')
    files = dict(self.data).keys()

    runDict = {}
    for f in files:
      #Get run_id from File Catalog
      #res = self.fc.getFileUserMetadata(f)
      #if not res['OK']:
      #  continue # Should we continue, or return S_ERROR
      #runID = res['Value']['run_id']
      #Get run_id manually from LFN
      split_LFN = f.split(os.path.sep)
      runID = split_LFN[6][4:]
      runDict.setdefault(runID, []).append(f)
    #t1 = time.time()
    #total = t1-t0
    #print('\trun_ids grouped in %s seconds.' %(str(total)))

    #c1 = 0
    #t2 = time.time()
    good_runDict = {}
    for runID in runDict:
      #print('Start with runID - %s' %(str(runID)))
      lfns = runDict[runID]
      if not lfns:
        continue
      inputDataQuery = {'DataType': 'Data', 'DataLevel': 'RAW', 'run_id': runID}
      #result = self.fc.findFilesByMetadata( inputDataQuery )
      dirName = os.path.split(lfns[0])[0]
      dataLFN = dirName.replace('ts_processed', 'data')
      dataLFN = dataLFN[0:dataLFN.find('katydid')]
      result = self.fc.listDirectory(dataLFN)
      #result = self.fc.getDatasetFiles(os.path.split(lfns[0])[0])
      #t3 = time.time()
      #total2 = t3-t2
      if not result['OK']:          
        print('count not get raw files')
        continue

      #print('\tFC Query #%s ran in %s seconds - %s' %(str(c1), str(total2), 'listDirectory'))
      #c1 = c1 + 1
      files = []
      file_list = result['Value']['Successful'][dataLFN]['Files'].keys()
      if result['OK']:
        files = file_list
        filtered_file = list({f for f in files if 'snapshot.json' in f})
      if not filtered_file:
        continue
      #t4 = time.time()
      res = self.fc.getDirectoryUserMetadata(dirName)
      if not res['OK']:
        continue
      #t5 = time.time()
      #total3 = t5-t4
      #print('\tFC Query #%s ran in %s seconds - %s' %(str(c1), str(total3), 'getDirectoryUserMetadata'))
      #c1 = c1 + 1
      metadata = res['Value']
      #pdb.set_trace()
      split_LFN = lfns[0].split(os.path.sep)
      # Checking if all egg files have been processed before merging.
      expectedrootlist = []
      for file in file_list:
        if '.egg' in file:
          path, filename = os.path.split(file)
          expectedrootlist.append(filename[:-4] + '_gain.root')
      inputDataQuery = {'run_id': metadata['run_id'], 'DataType': 'Data', 'DataLevel': 'processed', 'DataExt': 'root', 'SoftwareVersion': metadata['SoftwareVersion'], 'ConfigVersion': metadata['ConfigVersion']}

      result = self.fc.findFilesByMetadata( inputDataQuery, path='/project8/dirac/ts_processed/000yyyxxx/' + runID.zfill(9)[0:-3] + 'xxx' + '/' + runID.zfill(9), timeout=600 )
      #t6 = time.time()
      #total4 = t6-t5
      #print('\tFC Query #%s ran in %s seconds - %s' %(str(c1), str(total4), 'findFilesByMetadata'))
      #c1 = c1 + 1
      if not result['OK']:
        print('Could not get processed LFN list')
      currootlist = []
      for elements in result['Value']:
        path, filename = os.path.split(elements)
        currootlist.append(filename)

      eggfilenotprocessed = False
      for rootfile in expectedrootlist:
        if rootfile not in currootlist:
          print( '%s not found in list.'%(rootfile))
          eggfilenotprocessed = True
      if eggfilenotprocessed:
          continue

      #For each run_id, get list of event files from catalog to match with input lfn list.
      inputDataQuery2 = {'run_id': metadata['run_id'], 'DataType': 'Data', 'DataFlavor': 'event', 'DataExt': 'root', 'SoftwareVersion': metadata['SoftwareVersion'], 'ConfigVersion': metadata['ConfigVersion']}
      print({'run_id': metadata['run_id'], 'DataType': 'Data', 'DataFlavor': 'event', 'DataExt': 'root', 'SoftwareVersion': metadata['SoftwareVersion'], 'ConfigVersion': metadata['ConfigVersion']})
      print('/project8/dirac/ts_processed/000yyyxxx/' + runID.zfill(9)[0:-3] + 'xxx' + '/' + runID.zfill(9))
      result = self.fc.findFilesByMetadata( inputDataQuery2, path='/project8/dirac/ts_processed/000yyyxxx/' + runID.zfill(9)[0:-3] + 'xxx' + '/' + runID.zfill(9), timeout=600 )
      if not result['OK']:
             print('Could not get metadata')
             continue
      #pdb.set_trace()
      result['Value'] = [ x for x in result['Value'] if "_gain.root" not in x ]
      #print(str(result['Value']))
      #print(str(runDict[runID]))
      #good_runDict[runID] = runDict[runID]
      if set(result['Value'])==(set(runDict[runID])):
        good_runDict[runID] = runDict[runID]
      else:
        print('List of event files from catalog do not match with input lfn list')
      #t7 = time.time()
      #total5 = t7-t6
      #print('\tFC Query #%s ran in %s seconds - %s' %(str(c1), str(total5), 'findFilesByMetadata'))
      #c1 = c1 + 1
    ops_dict = self.opsHelper.getOptionsDict('Transformations/')
    if not ops_dict['OK']:
      return ops_dict
    ops_dict = ops_dict['Value']
    PROD_DEST_DATA_SE = ops_dict.get('ProdDestDataSE', 'PNNL-PIC-SRM-SE')
    tasks = [(PROD_DEST_DATA_SE, good_runDict[runID]) for runID in good_runDict]
    return S_OK(tasks)
    print('Done with runID - %s' %(str(runID)))


  def _P8MergeWoChecks(self):
    """
    Generate a task that merges processed data
    """
    print('\t MT::Running P8Merge wo checks')
    files = dict(self.data).keys()

    runDict = {}
    for f in files:
      #Get run_id from File Catalog
      #res = self.fc.getFileUserMetadata(f)
      #if not res['OK']:
      #  continue # Should we continue, or return S_ERROR
      #runID = res['Value']['run_id']
      #Get run_id manually from LFN
      split_LFN = f.split(os.path.sep)
      runID = split_LFN[6][4:]
      runDict.setdefault(runID, []).append(f)
    #t1 = time.time()
    #total = t1-t0
    #print('\trun_ids grouped in %s seconds.' %(str(total)))

    #c1 = 0
    #t2 = time.time()
    good_runDict = {}
    for runID in runDict:
      print('Start with runID - %s' %(str(runID)))
      lfns = runDict[runID]
      if not lfns:
        continue
      inputDataQuery = {'DataType': 'Data', 'DataLevel': 'RAW', 'run_id': runID}
      #result = self.fc.findFilesByMetadata( inputDataQuery )
      dirName = os.path.split(lfns[0])[0]
      dataLFN = dirName.replace('ts_processed', 'data')
      dataLFN = dataLFN[0:dataLFN.find('katydid')]
      result = self.fc.listDirectory(dataLFN)
      #result = self.fc.getDatasetFiles(os.path.split(lfns[0])[0])
      #t3 = time.time()
      #total2 = t3-t2
      if not result['OK']:          
        print('count not get raw files')
        continue

      #print('\tFC Query #%s ran in %s seconds - %s' %(str(c1), str(total2), 'listDirectory'))
      #c1 = c1 + 1
      files = []
      file_list = result['Value']['Successful'][dataLFN]['Files'].keys()
      if result['OK']:
        files = file_list
        filtered_file = list({f for f in files if 'snapshot.json' in f})
      #if not filtered_file: #Removing check for snapshot file
      #  continue
      res = self.fc.getDirectoryUserMetadata(dirName)
      if not res['OK']:
        continue
      metadata = res['Value']
      split_LFN = lfns[0].split(os.path.sep)
      # Checking if all egg files have been processed before merging.
      expectedrootlist = []
      for file in file_list:
        if '.egg' in file:
          path, filename = os.path.split(file)
          expectedrootlist.append(filename[:-4] + '_gain.root')
      inputDataQuery = {'run_id': metadata['run_id'], 'DataType': 'Data', 'DataLevel': 'processed', 'DataExt': 'root', 'SoftwareVersion': metadata['SoftwareVersion'], 'ConfigVersion': metadata['ConfigVersion']}

      result = self.fc.findFilesByMetadata( inputDataQuery, path='/project8/dirac/ts_processed/000yyyxxx/' + runID.zfill(9)[0:-3] + 'xxx' + '/' + runID.zfill(9), timeout=600 )
      if not result['OK']:
        print('Could not get processed LFN list')
      currootlist = []
      for elements in result['Value']:
        path, filename = os.path.split(elements)
        currootlist.append(filename)

      eggfilenotprocessed = False
      for rootfile in expectedrootlist:
        if rootfile not in currootlist:
          print( '%s not found in list.'%(rootfile))
          eggfilenotprocessed = True
      #if eggfilenotprocessed: #Skipping check if egg file is processed
      #    continue

      #For each run_id, get list of event files from catalog to match with input lfn list.
      inputDataQuery2 = {'run_id': metadata['run_id'], 'DataType': 'Data', 'DataFlavor': 'event', 'DataExt': 'root'}
      result = self.fc.findFilesByMetadata( inputDataQuery2, path='/project8/dirac//000yyyxxx/' + runID.zfill(9)[0:-3] + 'xxx' + '/' + runID.zfill(9), timeout=600 )
      if not result['OK']:
             print('Could not get metadata')
             continue
      #pdb.set_trace()
      result['Value'] = [ x for x in result['Value'] if "_gain.root" not in x ]
      print(str(result['Value']))
      print(str(runDict[runID]))
      good_runDict[runID] = runDict[runID]
      #if set(result['Value'])==(set(runDict[runID])):
      #  good_runDict[runID] = runDict[runID]
      #else:
      #  print('List of event files from catalog do not match with input lfn list')
    ops_dict = self.opsHelper.getOptionsDict('Transformations/')
    if not ops_dict['OK']:
      return ops_dict
    ops_dict = ops_dict['Value']
    PROD_DEST_DATA_SE = ops_dict.get('ProdDestDataSE', 'PNNL-PIC-SRM-SE')
    tasks = [(PROD_DEST_DATA_SE, good_runDict[runID]) for runID in good_runDict]
    return S_OK(tasks)
    print('Done with runID - %s' %(str(runID)))




  def _BySize( self ):
    """ Alias for groupBySize
    """
    return self._groupBySize()

  def _groupBySize( self, files = None ):
    """
    Generate a task for a given amount of data at a (set of) SE
    """
    if not files:
      files = self.data
    else:
      files = dict( zip( files, [self.data[lfn] for lfn in files] ) )
    return self.util.groupBySize( files, self.params['Status'] )


  def _Broadcast( self ):
    """ This plug-in takes files found at the sourceSE and broadcasts to all (or a selection of) targetSEs.
    """
    if not self.params:
      return S_ERROR( "TransformationPlugin._Broadcast: The 'Broadcast' plugin requires additional parameters." )

    targetseParam = self.params['TargetSE']
    targetSEs = []
    sourceSEs = eval( self.params['SourceSE'] )
    if targetseParam.count( '[' ):
      targetSEs = eval( targetseParam )
    elif isinstance( targetseParam, list ):
      targetSEs = targetseParam
    else:
      targetSEs = [targetseParam]
    # sourceSEs = eval(self.params['SourceSE'])
    # targetSEs = eval(self.params['TargetSE'])
    destinations = int( self.params.get( 'Destinations', 0 ) )
    if destinations and ( destinations >= len( targetSEs ) ):
      destinations = 0

    status = self.params['Status']
    groupSize = self.params['GroupSize']  # Number of files per tasks

    fileGroups = getFileGroups( self.data )  # groups by SE
    targetSELfns = {}
    for replicaSE, lfns in fileGroups.items():
      ses = replicaSE.split( ',' )
      # sourceSites = self._getSitesForSEs(ses)
      atSource = False
      for se in ses:
        if se in sourceSEs:
          atSource = True
      if not atSource:
        continue

      for lfn in lfns:
        targets = []
        sources = self._getSitesForSEs( ses )
        random.shuffle( targetSEs )
        for targetSE in targetSEs:
          site = self._getSiteForSE( targetSE )['Value']
          if not site in sources:
            if ( destinations ) and ( len( targets ) >= destinations ):
              continue
            sources.append( site )
          targets.append( targetSE )  # after all, if someone wants to copy to the source, it's his choice
        strTargetSEs = str.join( ',', sorted( targets ) )
        if not targetSELfns.has_key( strTargetSEs ):
          targetSELfns[strTargetSEs] = []
        targetSELfns[strTargetSEs].append( lfn )
    tasks = []
    for ses, lfns in targetSELfns.items():
      tasksLfns = breakListIntoChunks( lfns, groupSize )
      for taskLfns in tasksLfns:
        if ( status == 'Flush' ) or ( len( taskLfns ) >= int( groupSize ) ):
          # do not allow groups smaller than the groupSize, except if transformation is in flush state
          tasks.append( ( ses, taskLfns ) )
    return S_OK( tasks )

  def _ByShare( self, shareType = 'CPU' ):
    """ first get the shares from the CS, and then makes the grouping looking at the history
    """
    res = self._getShares( shareType, normalise = True )
    if not res['OK']:
      return res
    cpuShares = res['Value']
    self.util.logInfo( "Obtained the following target shares (%):" )
    for site in sorted( cpuShares.keys() ):
      self.util.logInfo( "%s: %.1f" % ( site.ljust( 15 ), cpuShares[site] ) )

    # Get the existing destinations from the transformationDB
    res = self.util.getExistingCounters( requestedSites = cpuShares.keys() )
    if not res['OK']:
      self.util.logError( "Failed to get existing file share", res['Message'] )
      return res
    existingCount = res['Value']
    if existingCount:
      self.util.logInfo( "Existing site utilization (%):" )
      normalisedExistingCount = self.util._normaliseShares( existingCount.copy() )
      for se in sorted( normalisedExistingCount.keys() ):
        self.util.logInfo( "%s: %.1f" % ( se.ljust( 15 ), normalisedExistingCount[se] ) )

    # Group the input files by their existing replicas
    res = self.util.groupByReplicas( self.data, self.params['Status'] )
    if not res['OK']:
      return res
    replicaGroups = res['Value']

    tasks = []
    # For the replica groups
    for replicaSE, lfns in replicaGroups:
      possibleSEs = replicaSE.split( ',' )
      # Determine the next site based on requested shares, existing usage and candidate sites
      res = self._getNextSite( existingCount, cpuShares, candidates = self._getSitesForSEs( possibleSEs ) )
      if not res['OK']:
        self.util.logError( "Failed to get next destination SE", res['Message'] )
        continue
      targetSite = res['Value']
      # Resolve the ses for the target site
      res = getSEsForSite( targetSite )
      if not res['OK']:
        continue
      ses = res['Value']
      # Determine the selected SE and create the task
      for chosenSE in ses:
        if chosenSE in possibleSEs:
          tasks.append( ( chosenSE, lfns ) )
          if not existingCount.has_key( targetSite ):
            existingCount[targetSite] = 0
          existingCount[targetSite] += len( lfns )
    return S_OK( tasks )

  def _getShares( self, shareType, normalise = False ):
    """ Takes share from the CS, eventually normalize them
    """
    res = gConfig.getOptionsDict( '/Resources/Shares/%s' % shareType )
    if not res['OK']:
      return res
    if not res['Value']:
      return S_ERROR( "/Resources/Shares/%s option contains no shares" % shareType )
    shares = res['Value']
    for site, value in shares.items():
      shares[site] = float( value )
    if normalise:
      shares = self.util._normaliseShares( shares )
    if not shares:
      return S_ERROR( "No non-zero shares defined" )
    return S_OK( shares )

  def _getNextSite( self, existingCount, targetShares, candidates = None ):
    if candidates is None:
      candidates = targetShares
    # normalise the existing counts
    existingShares = self.util._normaliseShares( existingCount )
    # then fill the missing share values to 0
    for site in targetShares:
      existingShares.setdefault( site, 0.0 )
    # determine which site is farthest from its share
    chosenSite = ''
    minShareShortFall = -float( "inf" )
    for site, targetShare in targetShares.items():
      if site not in candidates or not targetShare:
        continue
      existingShare = existingShares[site]
      shareShortFall = targetShare - existingShare
      if shareShortFall > minShareShortFall:
        minShareShortFall = shareShortFall
        chosenSite = site
    return S_OK( chosenSite )


  @classmethod
  def _getSiteForSE( cls, se ):
    """ Get site name for the given SE
    """
    result = getSitesForSE( se )
    if not result['OK']:
      return result
    if result['Value']:
      return S_OK( result['Value'][0] )
    return S_OK( '' )

  @classmethod
  def _getSitesForSEs( cls, seList ):
    """ Get all the sites for the given SE list
    """
    sites = []
    for se in seList:
      result = getSitesForSE( se )
      if result['OK']:
        sites += result['Value']
    return sites
