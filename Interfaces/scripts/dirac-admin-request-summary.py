#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-admin-request-summary
# Author :  Stuart Paterson
########################################################################
__RCSID__ = "1fc1232 (2010-12-14 13:18:48 +0000) Ricardo Graciani <graciani@ecm.ub.es>"
import DIRAC
from DIRAC.Core.Base import Script

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

from DIRAC.Interfaces.API.DiracAdmin                         import DiracAdmin
diracAdmin = DiracAdmin()

result = diracAdmin.getRequestSummary( printOutput = True )
if result['OK']:
  DIRAC.exit( 0 )
else:
  print result['Message']
  DIRAC.exit( 2 )

