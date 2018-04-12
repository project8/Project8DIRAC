#!/usr/bin/env python
########################################################################
# File :    dirac-admin-proxy-upload.py
# Author :  Adrian Casajus
###########################################################from DIRAC.Core.Base import Script#############

import sys
from DIRAC.Core.Base import Script
from DIRAC.FrameworkSystem.Client.ProxyUpload import CLIParams, uploadProxy

__RCSID__ = "4b89d17 (2016-11-28 16:21:16 +0100) Federico Stagni <federico.stagni@cern.ch>"

if __name__ == "__main__":
  cliParams = CLIParams()
  cliParams.registerCLISwitches()

  Script.parseCommandLine()

  retVal = uploadProxy( cliParams )
  if not retVal[ 'OK' ]:
    print retVal[ 'Message' ]
    sys.exit( 1 )
  sys.exit( 0 )
