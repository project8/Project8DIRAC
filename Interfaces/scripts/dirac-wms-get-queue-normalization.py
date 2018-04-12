#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-wms-get-queue-normalization.py
# Author :  Ricardo Graciani
########################################################################
"""
  Report Normalization Factor applied by Site to the given Queue
"""
__RCSID__ = "14bfb47 (2010-12-15 09:52:01 +0000) Ricardo Graciani <graciani@ecm.ub.es>"

import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.WorkloadManagementSystem.Client.CPUNormalization import getQueueNormalization

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... Queue ...' % Script.scriptName,
                                     'Arguments:',
                                     '  Queue:     GlueCEUniqueID of the Queue (ie, juk.nikhef.nl:8443/cream-pbs-lhcb)' ] ) )
Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

if len( args ) < 1:
  Script.showHelp()

exitCode = 0

for ceUniqueID in args:

  cpuNorm = getQueueNormalization( ceUniqueID )

  if not cpuNorm['OK']:
    print 'ERROR %s:' % ceUniqueID, cpuNorm['Message']
    exitCode = 2
    continue
  print ceUniqueID, cpuNorm['Value']

DIRAC.exit( exitCode )

