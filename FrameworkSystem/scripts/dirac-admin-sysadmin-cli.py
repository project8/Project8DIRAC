#!/usr/bin/env python
from DIRAC.Core.Base import Script

__RCSID__ = "78cc21a (2016-04-22 12:27:17 +0200) Federico Stagni <federico.stagni@cern.ch>"

host = None
Script.registerSwitch( "H:", "host=", "   Target host" )
Script.parseCommandLine( ignoreErrors = False )
for switch in Script.getUnprocessedSwitches():
  if switch[0].lower() == "h" or switch[0].lower() == "host":
    host = switch[1]

from DIRAC.FrameworkSystem.Client.SystemAdministratorClientCLI import SystemAdministratorClientCLI

cli = SystemAdministratorClientCLI( host )
cli.cmdloop()
